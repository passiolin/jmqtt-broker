/*
 * Copyright (c) 2026 ipuff.online
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package online.ipuff.jmqtt.admin;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.Channel;
import javax.annotation.PreDestroy;
import online.ipuff.jmqtt.cluster.ClusterBus;
import online.ipuff.jmqtt.cluster.ClusterBusStats;
import online.ipuff.jmqtt.config.BrokerProperties;
import online.ipuff.jmqtt.handler.ChannelAttributes;
import online.ipuff.jmqtt.message.SubscribeStore;
import online.ipuff.jmqtt.protocol.ConnectOptions;
import online.ipuff.jmqtt.redis.RedisConnectionManager;
import online.ipuff.jmqtt.session.BackpressureMetrics;
import online.ipuff.jmqtt.session.ConnectionRegistry;
import online.ipuff.jmqtt.session.ISessionStoreService;
import online.ipuff.jmqtt.session.QosMetrics;
import online.ipuff.jmqtt.session.SendBuffer;
import online.ipuff.jmqtt.session.SessionStore;
import online.ipuff.jmqtt.store.IPendingMessageStore;
import online.ipuff.jmqtt.store.InflightPersistence;
import online.ipuff.jmqtt.subscribe.ISubscribeStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 把本节点的管理视图发布到 Redis, 供控制台直接读取。
 *
 * <h2>三种数据的更新方式, 各自不同</h2>
 * <table>
 *   <tr><th>数据</th><th>写入方式</th><th>为什么</th></tr>
 *   <tr><td>节点概要</td><td>心跳周期全量覆盖</td>
 *       <td>字段少、变化慢, 全量覆盖最简单; 它同时承担「节点还活着」的信号</td></tr>
 *   <tr><td>客户端注册表</td><td>连接/断开时增量 HSET/HDEL</td>
 *       <td>十万级条目, 全量重写太贵; 单条变更本身很便宜</td></tr>
 *   <tr><td>订阅过滤器视图</td><td>变更后从主题树重算</td>
 *       <td>见下</td></tr>
 * </table>
 *
 * <h2>过滤器视图为什么是「重算」而不是「增量计数」</h2>
 * 增量计数的诱惑很大: 订阅时 {@code HINCRBY +1}, 退订时 {@code -1}, 开销极小。但它有一个
 * 无法回避的问题 —— <b>同一件事可能被写两次</b>。会话从持久层恢复时会把订阅重新写回本地主题树
 * (那批订阅其实早就在了), 增量计数会把它算成新增; 节点重启后本地树是空的, 增量计数却从
 * Redis 上的旧值继续累加。于是计数只会越漂越远, 而漂移是不可自愈的:
 * 没有任何时刻能确定「现在这个数是对的」。
 *
 * <p>改成从本地主题树重算之后, <b>漂移在结构上不可能发生</b> —— 树是唯一事实来源,
 * 每次算出来的都是当时的真值。代价是一次遍历, 所以用订阅变更版本号做闸门:
 * 版本没变就直接跳过, 空闲节点不做任何无用功; 变更后按
 * {@code filterPublishIntervalMs} 合并多次变更, 只算一次。
 *
 * <h2>绝不阻塞 MQTT 处理路径</h2>
 * 连接建立/断开只做一件事: <b>往有界队列里塞一个元素</b>, 然后立刻返回。
 * 真正的 Redis 写入在独立的守护线程上以批量管道的方式完成。
 * 队列满时丢弃并计数 —— 丢掉的增量不会造成永久偏差, 因为后续任何一次对账
 * (启动时、Redis 重连后、发现丢弃时)都会用全量重写把它纠正回来。
 */
@Component
public class AdminStatePublisher {

    private static final Logger log = LoggerFactory.getLogger(AdminStatePublisher.class);

    /** 一次批量写入最多合并多少条客户端变更 */
    private static final int FLUSH_BATCH = 2048;

    /** 对账时每多少个客户端提交一次 HSET, 避免构造超大命令把内存撑爆 */
    private static final int RECONCILE_CHUNK = 2000;

    /** 出站队列非空时的等待上限(毫秒); 决定空闲时的唤醒粒度 */
    private static final long IDLE_POLL_MS = 200;

    /**
     * 一条客户端状态变更。
     *
     * @param clientId 客户端标识
     * @param json     连接元数据; 为 {@code null} 表示该客户端已离线, 需从注册表删除
     */
    private record ClientOp(String clientId, String json) {
    }

    private final AdminProperties properties;
    private final BrokerProperties brokerProperties;
    private final AdminRedisKeys keys;
    private final ObjectProvider<RedisConnectionManager> redis;
    private final ConnectionRegistry connectionRegistry;
    private final ISessionStoreService sessionStoreService;
    private final ISubscribeStoreService subscribeStoreService;
    private final IPendingMessageStore pendingMessageStore;
    private final BackpressureMetrics backpressureMetrics;
    private final QosMetrics qosMetrics;
    private final ClusterBus clusterBus;
    private final ObjectProvider<ClusterBusStats> clusterBusStats;
    private final ObjectProvider<InflightPersistence> inflightPersistence;

    /** 用非 ASCII 转义之外的最紧凑形式: 字段名短, 十万条时差值很可观 */
    private final ObjectMapper objectMapper = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    private final BlockingQueue<ClientOp> outbox;
    private final AtomicLong droppedOps = new AtomicLong();
    private final AtomicLong lastReconciledDropCount = new AtomicLong();
    private final AtomicBoolean reconcileRequested = new AtomicBoolean(true);

    /**
     * 上次发布进客户端条目的 filters 签名(clientId → 规范化串), 供周期闸门比对。
     * 只在内存: 订阅变化后需要知道「谁变了」, 记住上次发过什么即可。
     */
    private final java.util.concurrent.ConcurrentHashMap<String, String> lastPublishedFilters =
            new java.util.concurrent.ConcurrentHashMap<>();

    private volatile long lastSubscribeVersion = -1L;
    private volatile long lastHeartbeatAt;
    private volatile long lastFilterPublishAt;
    private volatile boolean running = true;

    private final Thread worker;
    private final String nodeId;
    private final long startedAt = System.currentTimeMillis();

    public AdminStatePublisher(AdminProperties properties,
                               BrokerProperties brokerProperties,
                               AdminRedisKeys keys,
                               ObjectProvider<RedisConnectionManager> redis,
                               ConnectionRegistry connectionRegistry,
                               ISessionStoreService sessionStoreService,
                               ISubscribeStoreService subscribeStoreService,
                               IPendingMessageStore pendingMessageStore,
                               BackpressureMetrics backpressureMetrics,
                               QosMetrics qosMetrics,
                               ClusterBus clusterBus,
                               ObjectProvider<ClusterBusStats> clusterBusStats,
                               ObjectProvider<InflightPersistence> inflightPersistence) {
        this.properties = properties;
        this.brokerProperties = brokerProperties;
        this.nodeId = brokerProperties.id();
        this.keys = keys;
        this.redis = redis;
        this.connectionRegistry = connectionRegistry;
        this.sessionStoreService = sessionStoreService;
        this.subscribeStoreService = subscribeStoreService;
        this.pendingMessageStore = pendingMessageStore;
        this.backpressureMetrics = backpressureMetrics;
        this.qosMetrics = qosMetrics;
        this.clusterBus = clusterBus;
        this.clusterBusStats = clusterBusStats;
        this.inflightPersistence = inflightPersistence;
        this.outbox = new ArrayBlockingQueue<>(properties.outboxCapacity());
        this.worker = new Thread(this::loop, "jmqtt-admin-state");
        this.worker.setDaemon(true);
        this.worker.start();
    }

    // ------------------------------------------------------------------
    // 对外的三个变更入口(全部非阻塞)
    // ------------------------------------------------------------------

    /**
     * 某客户端<b>当下</b>的完整状态快照(含订阅列表), 供管理命令按需查询。
     *
     * <p>订阅不随变化实时上报 —— 重连风暴下的写放大不配「人工点开详情」这个低频动作;
     * 控制台需要时通过命令通道下发查询, 这里现场构建。{@code active()} 为 false 时
     * 照常构建: 它只读本地状态, 不触碰 Redis。
     *
     * @return 快照 JSON; 客户端不在线(或状态残缺)返回 {@code null}
     */
    public String clientSnapshotJson(String clientId) {
        if (clientId == null) {
            return null;
        }
        Channel channel = connectionRegistry.get(clientId);
        if (channel == null) {
            return null;
        }
        return buildClientJson(clientId, channel);
    }

    /**
     * 客户端已接入。由 CONNECT 处理路径调用。
     *
     * <p>这个方法<b>必须保持非阻塞且不抛异常</b>: 它在每条连接的建立路径上。
     */
    public void clientOnline(String clientId, Channel channel) {
        if (!active() || clientId == null || channel == null) {
            return;
        }
        try {
            enqueue(new ClientOp(clientId, buildClientJson(clientId, channel)));
        } catch (Exception e) {
            // 元数据解析失败不该影响连接建立 —— 放弃这条增量, 交给对账纠正
            countDrop();
            log.debug("构建客户端状态失败, 已跳过 clientId={}", clientId, e);
        }
    }

    /**
     * 客户端已断开。由连接关闭路径调用。
     *
     * <p><b>调用方必须先确认注册表里移除的确实是这条连接。</b>
     * 否则「旧连接延迟关闭」的回调会删掉刚接管它的新连接的状态 ——
     * 表现为控制台里客户端凭空消失, 而它其实好好地在线。
     */
    public void clientOffline(String clientId) {
        lastPublishedFilters.remove(clientId);
        if (!active() || clientId == null) {
            return;
        }
        enqueue(new ClientOp(clientId, null));
    }

    /**
     * 请求一次全量对账。用于启动、Redis 重连、以及发现增量被丢弃时。
     */
    public void requestReconcile() {
        reconcileRequested.set(true);
    }

    // ------------------------------------------------------------------
    // 工作线程
    // ------------------------------------------------------------------

    private void loop() {
        while (running) {
            try {
                ClientOp first = outbox.poll(IDLE_POLL_MS, TimeUnit.MILLISECONDS);
                if (first != null) {
                    List<ClientOp> batch = new ArrayList<>(FLUSH_BATCH);
                    batch.add(first);
                    outbox.drainTo(batch, FLUSH_BATCH - 1);
                    flushClientOps(batch);
                }
                long now = System.currentTimeMillis();
                if (now - lastHeartbeatAt >= properties.heartbeatIntervalMs()) {
                    heartbeat(now);
                }
                if (now - lastFilterPublishAt >= properties.filterPublishIntervalMs()) {
                    publishFilters(now);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.warn("管理面状态发布出错(不影响 MQTT 处理)", e);
            }
        }
    }

    private void flushClientOps(List<ClientOp> batch) {
        RedisConnectionManager manager = redis.getIfAvailable();
        if (manager == null) {
            return;
        }
        String key = keys.clients(nodeId);
        manager.execute("admin.clientRegistry", () -> {
            Map<String, String> puts = new LinkedHashMap<>();
            List<String> dels = new ArrayList<>();
            for (ClientOp op : batch) {
                if (op.json() == null) {
                    dels.add(op.clientId());
                } else {
                    puts.put(op.clientId(), op.json());
                }
            }
            if (!dels.isEmpty()) {
                manager.commands().hdel(key, dels.toArray(new String[0]));
            }
            if (!puts.isEmpty()) {
                manager.commands().hset(key, puts);
            }
            return null;
        }, null);
    }

    /**
     * 心跳: 写节点概要, 并给三个状态键续期。
     *
     * <p>续期这一步容易被忽略: Redis 里 {@code HSET} <b>不会</b>刷新键的 TTL,
     * 所以「持续写入」并不等于「键不会过期」。节点若忘了显式 EXPIRE,
     * 数据会在 TTL 到点后整体消失, 而节点还在正常跑 —— 表现为控制台里节点时有时无。
     */
    private void heartbeat(long now) {
        lastHeartbeatAt = now;
        RedisConnectionManager manager = redis.getIfAvailable();
        if (manager == null) {
            return;
        }
        if (!properties.enabled()) {
            return;
        }
        Map<String, String> summary = buildSummary(now);
        int ttl = properties.effectiveStateTtlSeconds();
        manager.execute("admin.heartbeat", () -> {
            manager.commands().sadd(keys.nodes(), nodeId);
            manager.commands().hset(keys.node(nodeId), summary);
            manager.commands().expire(keys.node(nodeId), ttl);
            manager.commands().expire(keys.clients(nodeId), ttl);
            manager.commands().expire(keys.filters(nodeId), ttl);
            return null;
        }, null);

        // 上一轮有增量被丢弃 -> 用一次全量对账消除偏差
        if (reconcileRequested.get() || droppedOps.get() != lastReconciledDropCount.get()) {
            reconcile();
        }
    }

    /**
     * 全量重写客户端注册表。
     *
     * <p>先 {@code DEL} 再分块写回。中间若 Redis 断掉, 会留下一个不完整的注册表 ——
     * 这没关系: 键有 TTL, 且下一次对账会重来。反过来, 若不做 DEL 只做增量补齐,
     * 「曾经连过但后来因故没发出 HDEL」的条目就永远留在那里成为幽灵客户端。
     */
    private void reconcile() {
        RedisConnectionManager manager = redis.getIfAvailable();
        if (manager == null) {
            return;
        }
        Set<String> live = connectionRegistry.clientIds();
        String key = keys.clients(nodeId);
        int ttl = properties.effectiveStateTtlSeconds();
        Boolean ok = manager.execute("admin.reconcile", () -> {
            manager.commands().del(key);
            Map<String, String> chunk = new LinkedHashMap<>(RECONCILE_CHUNK);
            int count = 0;
            for (String clientId : live) {
                Channel channel = connectionRegistry.get(clientId);
                if (channel == null) {
                    // 快照与读取之间断开了, 跳过即可 —— 它本来也不该在结果里
                    continue;
                }
                chunk.put(clientId, buildClientJson(clientId, channel));
                if (++count % RECONCILE_CHUNK == 0) {
                    manager.commands().hset(key, chunk);
                    chunk.clear();
                }
            }
            if (!chunk.isEmpty()) {
                manager.commands().hset(key, chunk);
            }
            manager.commands().expire(key, ttl);
            return Boolean.TRUE;
        }, null);
        if (Boolean.TRUE.equals(ok)) {
            lastReconciledDropCount.set(droppedOps.get());
            reconcileRequested.set(false);
            log.info("管理面客户端注册表已对账: 节点={} 条目={} 累计丢弃增量={}",
                    nodeId, live.size(), droppedOps.get());
        }
    }

    /**
     * 重算并发布订阅过滤器视图。
     *
     * <p>用变更版本号做闸门, 空闲时直接返回 —— 一次遍历在十万级订阅下是实打实的 CPU,
     * 不能每秒无脑做一遍。
     */
    private void publishFilters(long now) {
        if (!properties.enabled()) {
            return;
        }
        long version = subscribeStoreService.mutationVersion();
        if (version == lastSubscribeVersion) {
            return;
        }
        RedisConnectionManager manager = redis.getIfAvailable();
        if (manager == null) {
            return;
        }
        Map<String, Integer> histogram = subscribeStoreService.filterHistogram();
        String key = keys.filters(nodeId);
        int ttl = properties.effectiveStateTtlSeconds();
        Boolean ok = manager.execute("admin.filters", () -> {
            manager.commands().del(key);
            if (!histogram.isEmpty()) {
                Map<String, String> chunk = new LinkedHashMap<>(RECONCILE_CHUNK);
                for (Map.Entry<String, Integer> entry : histogram.entrySet()) {
                    chunk.put(entry.getKey(), String.valueOf(entry.getValue()));
                    if (chunk.size() >= RECONCILE_CHUNK) {
                        manager.commands().hset(key, chunk);
                        chunk.clear();
                    }
                }
                if (!chunk.isEmpty()) {
                    manager.commands().hset(key, chunk);
                }
            }
            manager.commands().expire(key, ttl);
            return Boolean.TRUE;
        }, null);
        if (Boolean.TRUE.equals(ok)) {
            lastSubscribeVersion = version;
            lastFilterPublishAt = now;
            refreshChangedClients();
        }
    }

    /**
     * 把<b>订阅确实发生了变化</b>的在线客户端条目重新入队。
     *
     * <p>客户端条目里的 {@code filters} 是连接时刻的快照, 而订阅发生在 CONNECT 之后 ——
     * 不补这一步, 控制台「点过滤器找订阅者」永远找不到人(扫的就是这个字段)。
     *
     * <p>挂在过滤器视图的变更闸门之后({@code mutationVersion} 没变直接返回):
     * <b>不是</b>每次 SUBSCRIBE 都写(重连风暴下的写放大不配低频查看),
     * 而是随周期重算(默认 5s)比对一次内存, 只有 filters 真变了的客户端才重发一条。
     * 空闲时成本是一次内存遍历, 零入队。
     */
    private void refreshChangedClients() {
        for (String clientId : connectionRegistry.clientIds()) {
            String current = filtersSignature(clientId);
            String published = lastPublishedFilters.get(clientId);
            if (published != null && published.equals(current)) {
                continue;
            }
            Channel channel = connectionRegistry.get(clientId);
            if (channel == null) {
                lastPublishedFilters.remove(clientId);
                continue;
            }
            try {
                enqueue(new ClientOp(clientId, buildClientJson(clientId, channel)));
                if (current != null) {
                    lastPublishedFilters.put(clientId, current);
                } else {
                    lastPublishedFilters.remove(clientId);
                }
            } catch (Exception e) {
                countDrop();
            }
        }
    }

    /** 客户端当前订阅的规范化串(排序后拼接), 作为「有没有变」的比对签名 */
    private String filtersSignature(String clientId) {
        java.util.Collection<SubscribeStore> subscriptions = subscribeStoreService.subscriptionsOf(clientId);
        if (subscriptions.isEmpty()) {
            return null;
        }
        return subscriptions.stream()
                .map(SubscribeStore::topicFilter)
                .sorted()
                .collect(java.util.stream.Collectors.joining("\u0001"));
    }

    // ------------------------------------------------------------------
    // 组装
    // ------------------------------------------------------------------

    private Map<String, String> buildSummary(long now) {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("node", nodeId);
        fields.put("startedAt", String.valueOf(startedAt));
        fields.put("updatedAt", String.valueOf(now));
        fields.put("mqttPort", String.valueOf(brokerProperties.port()));
        fields.put("websocketEnabled", String.valueOf(brokerProperties.websocketEnabled()));
        fields.put("websocketPort", String.valueOf(brokerProperties.websocketPort()));
        fields.put("clusterEnabled", String.valueOf(brokerProperties.clusterEnabled()));
        fields.put("clusterBusEnabled", String.valueOf(clusterBus.enabled()));
        // 广播形态必须出现在节点概要里: 开关配错的表现是「消息静默不到」,
        // 没有任何报错, 因此它是运维唯一能在控制台上一眼确认这件事的地方
        fields.put("broadcastMode", broadcastMode());
        fields.put("uplinkEnabled", String.valueOf(clusterBus.uplinkEnabled()));
        fields.put("sessionExpirySeconds", String.valueOf(brokerProperties.sessionExpirySeconds()));
        fields.put("maxInflight", String.valueOf(brokerProperties.maxInflight()));

        // 已注册的连接数(clientId -> Channel)
        fields.put("connections", String.valueOf(connectionRegistry.size()));
        // TCP 层连接数。与上一项的差值 = 建立了 TCP 但尚未完成 CONNECT 的连接,
        // 这是排查「有人在连但不发 CONNECT」的唯一线索
        fields.put("rawConnections", String.valueOf(rawConnectionCount()));
        fields.put("sessions", String.valueOf(sessionStoreService.size()));
        fields.put("subscriptions", String.valueOf(subscribeStoreService.subscriptionCount()));
        fields.put("topicNodes", String.valueOf(subscribeStoreService.topicNodeCount()));
        fields.put("offlineQueuePersistent", String.valueOf(pendingMessageStore.persistent()));
        fields.put("offlineMessagesDropped", String.valueOf(pendingMessageStore.droppedCount()));

        // 背压与 QoS: 控制台要能一眼看出节点是否正在降级
        appendAll(fields, "backpressure", backpressureMetrics.stats());
        appendAll(fields, "qos", qosMetrics.stats());

        ClusterBusStats busStats = clusterBusStats.getIfAvailable();
        if (busStats != null) {
            appendAll(fields, "clusterBus", busStats.stats());
        }
        InflightPersistence inflight = inflightPersistence.getIfAvailable();
        if (inflight != null) {
            appendAll(fields, "inflight", inflight.stats());
        }
        return fields;
    }

    /**
     * 集群广播当前的实际形态。
     */
    private String broadcastMode() {
        BrokerProperties.KafkaProperties kafka = brokerProperties.kafka();
        if (!brokerProperties.clusterEnabled()) {
            return "cluster-disabled";
        }
        if (!kafka.broadcastEnabled()) {
            return "off";
        }
        return kafka.broadcastAll() ? "all" : "filtered";
    }

    private int rawConnectionCount() {
        return connectionRegistry.channels().size();
    }

    private void appendAll(Map<String, String> target, String section, Map<String, ?> source) {
        if (source == null) {
            return;
        }
        source.forEach((name, value) -> {
            if (value != null) {
                target.put(section + "." + name, String.valueOf(value));
            }
        });
    }

    /**
     * 单个客户端的元数据。字段名刻意取短 —— 十万条记录下, 每个字段名省下的字节
     * 都会乘以十万。取值全部来自连接上的属性或本地会话, 不产生任何外部调用。
     */
    private String buildClientJson(String clientId, Channel channel) {
        Map<String, Object> view = new LinkedHashMap<>(16);
        view.put("clientId", clientId);
        if (channel.remoteAddress() != null) {
            view.put("addr", channel.remoteAddress().toString());
        }
        ConnectOptions options = channel.attr(ChannelAttributes.CONNECT_OPTIONS).get();
        if (options != null) {
            view.put("ver", options.protocolVersion());
            view.put("receiveMax", options.receiveMaximum());
        }
        view.put("keepAlive", attr(channel, ChannelAttributes.KEEP_ALIVE, 0));
        view.put("connectedAt", attr(channel, ChannelAttributes.CONNECTED_AT, 0L));

        SessionStore session = sessionStoreService.get(clientId);
        if (session != null) {
            view.put("persistent", session.isPersistent());
            view.put("expiry", session.getExpireSeconds());
            view.put("lastActiveAt", session.getLastActiveAt());
            view.put("hasWill", session.getWillMessage() != null);
        }

        Collection<SubscribeStore> subscriptions = subscribeStoreService.subscriptionsOf(clientId);
        view.put("subs", subscriptions.size());
        int cap = properties.maxFiltersPerClient();
        if (cap > 0 && !subscriptions.isEmpty()) {
            List<String> filters = new ArrayList<>(Math.min(cap, subscriptions.size()));
            for (SubscribeStore subscription : subscriptions) {
                if (filters.size() >= cap) {
                    break;
                }
                filters.add(subscription.topicFilter());
            }
            view.put("filters", filters);
            if (subscriptions.size() > cap) {
                view.put("filtersTruncated", true);
            }
        }

        SendBuffer sendBuffer = channel.attr(ChannelAttributes.SEND_BUFFER).get();
        if (sendBuffer != null) {
            // 有积压的客户端是排查投递问题的第一入口, 值得随连接状态一起上报
            int inflight = sendBuffer.inflightCount();
            int queued = sendBuffer.queuedCount();
            if (inflight > 0) {
                view.put("inflight", inflight);
            }
            if (queued > 0) {
                view.put("queued", queued);
            }
        }
        try {
            return objectMapper.writeValueAsString(view);
        } catch (JsonProcessingException e) {
            // 序列化 Map<String, Object> 理论上不会失败(值都是字符串/数字/布尔/列表)。
            // 但真失败了也必须给出一条记录: 少几个字段只是信息不全,
            // 而返回 null 会让这个客户端从控制台上凭空消失 —— 后者会让人误判。
            log.debug("序列化客户端状态失败, 退化为最小记录 clientId={}", clientId, e);
            return "{\"clientId\":\"" + escape(clientId) + "\"}";
        }
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static <T> T attr(Channel channel, io.netty.util.AttributeKey<T> key, T fallback) {
        T value = channel.attr(key).get();
        return value == null ? fallback : value;
    }

    // ------------------------------------------------------------------

    private boolean active() {
        return properties.enabled() && redis.getIfAvailable() != null;
    }

    private void enqueue(ClientOp op) {
        if (!outbox.offer(op)) {
            countDrop();
        }
    }

    private void countDrop() {
        long total = droppedOps.incrementAndGet();
        // 只在第一次和每十万次打日志: 出站队列打满时日志本身会成为新的压力源
        if (total == 1 || total % 100_000 == 0) {
            log.warn("管理面状态出站队列已满, 累计丢弃 {} 条增量(不影响 MQTT 处理, 稍后由对账纠正)", total);
        }
    }

    /** 供控制台 SNAPSHOT 命令与测试查询 */
    public long droppedOperations() {
        return droppedOps.get();
    }

    @PreDestroy
    public void shutdown() {
        running = false;
        worker.interrupt();
    }
}
