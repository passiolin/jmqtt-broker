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
package online.ipuff.jmqtt.cluster.kafka;

import online.ipuff.jmqtt.cluster.ClusterBus;
import online.ipuff.jmqtt.cluster.ClusterBusStats;
import online.ipuff.jmqtt.cluster.ConnectionEvent;
import online.ipuff.jmqtt.cluster.InternalMessage;
import online.ipuff.jmqtt.cluster.InternalSendServer;
import online.ipuff.jmqtt.config.BrokerProperties;
import online.ipuff.jmqtt.router.MqttTopic;
import online.ipuff.jmqtt.router.TopicTrie;
import online.ipuff.jmqtt.session.ConnectionEventTakeover;
import online.ipuff.jmqtt.subscribe.ISubscribeStoreService;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka 总线的一级入口: 路由决策 + 组件装配 + 统计聚合。
 *
 * <p>内部组件(职责分离, 各自独立启停):
 * <ul>
 *   <li>{@link KafkaMessageProducer} —— 生产者: 有界出站队列 + 发送线程</li>
 *   <li>{@link KafkaClusterConsumer} —— 消费集群广播(jmqtt-cluster)</li>
 *   <li>{@link KafkaDownlinkConsumer} —— 消费后台下行(downlink topics)</li>
 *   <li>{@link KafkaConnectionEventConsumer} —— 消费连接事件, 跨节点接管时释放旧连接</li>
 * </ul>
 *
 * <p>本类负责: 上行/广播的匹配与分流、记录编码(ClusterRecords)、生命周期、指标。
 * 入队永不阻塞发布路径; 消费线程各自容错自愈。
 */
@Component
@ConditionalOnProperty(prefix = "jmqtt.broker.kafka", name = "enabled", havingValue = "true")
public class KafkaClusterBus implements ClusterBus, ClusterBusStats, SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(KafkaClusterBus.class);

    private final BrokerProperties properties;
    private final KafkaMessageProducer producer;
    private final KafkaRecordSerializer serializer;
    private final KafkaClusterConsumer clusterConsumer;
    private final KafkaDownlinkConsumer downlinkConsumer;
    /** 跨节点接管触发侧; 仅 cluster-enabled 且配置了连接事件 topic 时存在 */
    private final ConnectionEventTakeover takeoverHandler;
    private final KafkaConnectionEventConsumer eventConsumer;

    private volatile boolean running;

    /** 上行过滤器匹配树; 过滤器为空时 uplinkAll = true */
    private final TopicTrie<Boolean> uplinkTrie = new TopicTrie<>();
    private volatile boolean uplinkAll;

    /**
     * 多路由上行(routes 配置)。非空时取代单路由的 uplink-topic:
     * 一条消息命中多条路由就各发一份, 每条路由有自己的目标 topic 与分区键。
     */
    private final java.util.List<RouteMatcher> routeMatchers = new java.util.ArrayList<>();

    /** 广播过滤器匹配树(与上行过滤器同一套通配符语义) */
    private final TopicTrie<Boolean> broadcastTrie = new TopicTrie<>();
    private volatile boolean broadcastAll = true;

    private final AtomicLong uplinkCount = new AtomicLong();
    private final AtomicLong broadcastSkippedCount = new AtomicLong();

    public KafkaClusterBus(BrokerProperties properties,
                           InternalSendServer internalSendServer,
                           ISubscribeStoreService subscribeStoreService) {
        this(properties, internalSendServer, subscribeStoreService, null);
    }

    @Autowired
    public KafkaClusterBus(BrokerProperties properties,
                           InternalSendServer internalSendServer,
                           ISubscribeStoreService subscribeStoreService,
                           ConnectionEventTakeover connectionEventTakeover) {
        this.properties = properties;
        this.producer = new KafkaMessageProducer(properties);
        this.serializer = new KafkaRecordSerializer(producer);
        this.clusterConsumer = new KafkaClusterConsumer(properties, internalSendServer, subscribeStoreService);
        List<String> downlinkTopics = KafkaDownlinkConsumer.topicsOf(properties.kafka());
        this.downlinkConsumer = downlinkTopics.isEmpty()
                ? null : new KafkaDownlinkConsumer(properties, internalSendServer, downlinkTopics);
        this.takeoverHandler = connectionEventTakeover;
        String eventTopic = properties.kafka().connectionEventTopic();
        // 单节点不需要跨节点接管; 没有事件 topic 时也无从发现 - 两种情况都不装配,
        // 旧连接继续由 keepalive 超时兜底(行为与改造前一致)
        boolean eventTakeover = properties.clusterEnabled() && connectionEventTakeover != null
                && eventTopic != null && !eventTopic.isBlank();
        this.eventConsumer = eventTakeover
                ? new KafkaConnectionEventConsumer(properties, connectionEventTakeover, eventTopic)
                : null;
        initUplinkMatcher();
        initBroadcastMatcher();
        initRouteMatchers();
    }

    /** 测试用: 出站队列(同包可见, 校验 record 结构)。 */
    BlockingQueue<ProducerRecord<String, byte[]>> outbox() {
        return producer.outbox;
    }

    /** 测试用: 等异步编码把记录都放进 outbox。 */
    void flushSerialization() throws InterruptedException {
        serializer.flush();
    }

    /** 解析多路由上行配置; 非法过滤器忽略并告警(与广播过滤器同一口径) */
    private void initRouteMatchers() {
        java.util.List<BrokerProperties.KafkaProperties.Route> routes = properties.kafka().routes();
        if (routes == null || routes.isEmpty()) {
            return;
        }
        for (BrokerProperties.KafkaProperties.Route route : routes) {
            if (route.topic() == null || route.topic().isBlank()) {
                log.warn("忽略未配置 topic 的上行路由: {}", route);
                continue;
            }
            routeMatchers.add(new RouteMatcher(route));
        }
        if (!routeMatchers.isEmpty()) {
            log.info("上行多路由已启用: {} 条(routes 配置取代 uplink-topic 单路由)",
                    routeMatchers.size());
        }
    }

    /**
     * 初始化广播过滤器。
     *
     * <p>三种形态要严格区分开, 因为它们对应完全不同的业务前提:
     * <ol>
     *   <li><b>关闭广播</b>(默认) —— 业务上不存在跨节点 MQTT 订阅, 消息只在本节点投递 + 走上行数据面。</li>
     *   <li><b>全部广播</b>(开启且未配置过滤器) —— 保持既有语义不变。</li>
     *   <li><b>按过滤器广播</b> —— 只有可能被其他节点订阅的主题才跨节点, 例如端与端之间的信令。</li>
     * </ol>
     * 第 2、3 种都靠「过滤器列表是否为空」区分, 所以第 1 种必须由独立的开关表达 ——
     * 不能复用「空列表」, 否则「关闭广播」与「配置漏了」会变成同一个状态。
     */
    private void initBroadcastMatcher() {
        BrokerProperties.KafkaProperties kafka = properties.kafka();
        if (!kafka.broadcastEnabled()) {
            log.info("集群广播已关闭(broadcast-enabled=false, 默认值)。本节点投递与数据面上行不受影响, "
                    + "但其他节点上的 MQTT 订阅者收不到本节点发布的消息。"
                    + "存在跨节点 MQTT 订阅时须置 true —— 这个开关一旦配错, "
                    + "表现是「消息静默不到」而不是报错, 因此控制台会把它标在节点概要里。");
            return;
        }
        List<String> filters = kafka.broadcastFilters();
        if (filters == null || filters.isEmpty()) {
            this.broadcastAll = true;
            return;
        }
        for (String filter : filters) {
            if (!MqttTopic.isValidFilter(filter)) {
                log.warn("忽略非法的广播过滤器: {}", filter);
                continue;
            }
            broadcastTrie.insert(MqttTopic.levels(filter), filter, Boolean.TRUE);
        }
        if (broadcastTrie.valueCount() == 0) {
            this.broadcastAll = true;
            log.warn("广播过滤器全部非法或为空 → 退化为全部主题都广播");
        } else {
            this.broadcastAll = false;
            log.info("集群广播已按过滤器收敛, 未匹配的主题不会跨节点: {}", filters);
        }
    }

    private void initUplinkMatcher() {
        BrokerProperties.KafkaProperties kafka = properties.kafka();
        if (!kafka.uplinkEnabled()) {
            this.uplinkAll = false;
            return;
        }
        List<String> filters = kafka.uplinkFilters();
        if (filters == null || filters.isEmpty()) {
            this.uplinkAll = true;
            log.info("上行数据面已启用, topic={}, 未配置过滤器 → 全部消息视为上行",
                    kafka.uplinkTopic());
            return;
        }
        for (String filter : filters) {
            if (!MqttTopic.isValidFilter(filter)) {
                log.warn("忽略非法的上行过滤器: {}", filter);
                continue;
            }
            uplinkTrie.insert(MqttTopic.levels(filter), filter, Boolean.TRUE);
        }
        if (uplinkTrie.valueCount() == 0) {
            this.uplinkAll = true;
            log.warn("上行过滤器全部非法或为空 → 退化为全部消息视为上行");
        } else {
            log.info("上行数据面已启用, topic={}, 过滤器={}", kafka.uplinkTopic(), filters);
        }
    }

    // ------------------------------------------------------------------
    // ClusterBus
    // ------------------------------------------------------------------

    @Override
    public boolean enabled() {
        return true;
    }

    @Override
    public void publish(InternalMessage message) {
        String topic = properties.kafka().clusterTopic();
        if (topic == null || topic.isEmpty()) {
            return;
        }
        // 消息面 key 恒为 MQTT 主题 —— 「同主题必落同分区, 分区内有序 ⇒ 同主题有序」
        // 这条链的起点。数据面的 uplink-key(按设备分区)只属于数据面, 不得泄漏进来:
        // 否则改一个数据面参数会悄悄破坏消息面的顺序保证。
        serializer.publishBroadcast(topic, message);
    }

    @Override
    public boolean uplinkEnabled() {
        return !routeMatchers.isEmpty() || properties.kafka().uplinkEnabled();
    }

    @Override
    public boolean isUplink(String topic) {
        if (!routeMatchers.isEmpty()) {
            if (topic == null || topic.isEmpty()) {
                return false;
            }
            for (RouteMatcher matcher : routeMatchers) {
                if (matcher.matches(topic)) {
                    return true;
                }
            }
            return false;
        }
        if (!uplinkEnabled() || topic == null || topic.isEmpty()) {
            return false;
        }
        if (uplinkAll) {
            return true;
        }
        return uplinkTrie.hasMatch(MqttTopic.levels(topic));
    }

    @Override
    public boolean isBroadcastAllowed(String topic) {
        if (!properties.kafka().broadcastEnabled()) {
            broadcastSkippedCount.incrementAndGet();
            return false;
        }
        if (broadcastAll) {
            return true;
        }
        if (topic == null || topic.isEmpty()) {
            return false;
        }
        boolean allowed = broadcastTrie.hasMatch(MqttTopic.levels(topic));
        if (!allowed) {
            // 计进指标而不是打日志: 关掉广播之后每一批遥测都会走到这里,
            // 打日志等于用日志把磁盘写满
            broadcastSkippedCount.incrementAndGet();
        }
        return allowed;
    }

    @Override
    public void publishUplink(InternalMessage message) {
        // 多路由: 命中一条发一份, 命中多条发多份; 每条路由有自己的分区键
        if (!routeMatchers.isEmpty()) {
            int sent = 0;
            for (RouteMatcher matcher : routeMatchers) {
                if (!matcher.matches(message.topic())) {
                    continue;
                }
                String key = matcher.route.keyByDevice()
                        && message.clientId() != null && !message.clientId().isBlank()
                        ? message.clientId() : message.topic();
                serializer.publishRouted(matcher.route.topic(), key, message);
                sent++;
            }
            if (sent > 0) {
                uplinkCount.incrementAndGet();
            }
            return;
        }
        uplinkCount.incrementAndGet();
        String topic = properties.kafka().uplinkTopic();
        if (topic == null || topic.isEmpty()) {
            return;
        }
        // 数据面分区键按 uplink-key 配置: topic(默认)或 device(每设备自身时序有序 + 流量散开)
        String partitionKey = properties.kafka().uplinkPartitionKey(message.clientId(), message.topic());
        serializer.publishRouted(topic, partitionKey, message);
    }

    @Override
    public void publishConnectionEvent(ConnectionEvent event) {
        String topic = properties.kafka().connectionEventTopic();
        if (topic == null || topic.isEmpty()) {
            return;
        }
        // key=clientId 由 connectionEventRecord 保证: 同一客户端的事件严格有序
        serializer.publishConnectionEvent(topic, event);
    }

    @Override
    public boolean takeoverSupported() {
        return eventConsumer != null;
    }

    // ------------------------------------------------------------------
    // 生命周期
    // ------------------------------------------------------------------

    @Override
    public void start() {
        if (running) {
            return;
        }
        BrokerProperties.KafkaProperties kafka = properties.kafka();
        String brokerId = properties.id();
        try {
            producer.start();
            serializer.start();
            clusterConsumer.start();
            if (downlinkConsumer != null) {
                downlinkConsumer.start();
            }
            if (eventConsumer != null) {
                eventConsumer.start();
            }
        } catch (Exception e) {
            // 启动期 Kafka 不可用不应导致 broker 起不来, 但必须让运维明确知道
            log.error("Kafka 客户端初始化失败, 集群功能不可用。本地投递不受影响", e);
            stopQuietly();
            return;
        }
        running = true;

        log.info("Kafka 集群总线已启动: clusterTopic={} groupId={} consumerThreads={} uplinkTopic={} uplinkExclusive={}",
                kafka.clusterTopic(), kafka.resolvedGroupId(brokerId), clusterConsumer.workerCount(),
                kafka.uplinkEnabled() ? kafka.uplinkTopic() : "(未启用)",
                kafka.uplinkExclusive());
        if (downlinkConsumer != null) {
            log.info("下行通道已启动: topics={} groupId={}(每节点独立, 只做本地投递) downlinkWorkers={}",
                    KafkaDownlinkConsumer.topicsOf(kafka), kafka.resolvedGroupId(brokerId) + "-downlink",
                    downlinkConsumer.workerCount());
        }
        if (eventConsumer != null) {
            log.info("连接事件消费已启动: topic={} groupId={}(每节点独立, 单 worker), "
                            + "跨节点接管由 connected 事件触发",
                    kafka.connectionEventTopic(), kafka.resolvedGroupId(brokerId) + "-events");
        } else if (properties.clusterEnabled()) {
            log.warn("集群已启用但跨节点接管未生效: connection-event-topic 未配置。"
                    + "客户端切换到其他节点后, 本节点旧连接会存活到自身心跳超时, "
                    + "期间同一 clientId 双在线(指令可能重复执行)");
        }
        if ("jmqtt".equals(brokerId)) {
            // 默认 id 是集群里最隐蔽的坑: 所有节点同 id ⇒ 同消费组(广播退化为分摊),
            // 且收端跳过"自己"的消息时会把每个节点的消息都当自己的丢掉 —— 集群全哑
            log.warn("broker.id 仍是默认值 [jmqtt]: 集群部署必须每节点一个唯一 id(配置 JMQTT_ID), "
                    + "否则跨节点投递静默失效");
        }
        log.info("注意: groupId 每个节点必须不同, 相同会退化为组内分摊而非广播。当前={}",
                kafka.resolvedGroupId(brokerId));
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        log.info("正在停止 Kafka 集群总线 ...");
        clusterConsumer.stop();
        if (downlinkConsumer != null) {
            downlinkConsumer.stop();
        }
        if (eventConsumer != null) {
            eventConsumer.stop();
        }
        serializer.stop();
        producer.stop();
        log.info("Kafka 集群总线已停止.");
    }

    private void stopQuietly() {
        try {
            clusterConsumer.stop();
        } catch (Exception e) {
            log.debug("停止集群消费者异常", e);
        }
        if (downlinkConsumer != null) {
            try {
                downlinkConsumer.stop();
            } catch (Exception e) {
                log.debug("停止下行消费者异常", e);
            }
        }
        if (eventConsumer != null) {
            try {
                eventConsumer.stop();
            } catch (Exception e) {
                log.debug("停止连接事件消费者异常", e);
            }
        }
        try {
            serializer.stop();
        } catch (Exception e) {
            log.debug("停止记录编码器异常", e);
        }
        try {
            producer.stop();
        } catch (Exception e) {
            log.debug("停止生产者异常", e);
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    // ------------------------------------------------------------------
    // 指标
    // ------------------------------------------------------------------

    @Override
    public Map<String, Long> stats() {
        Map<String, Long> stats = new LinkedHashMap<>();
        stats.put("published", producer.published());
        stats.put("dropped", producer.dropped());
        stats.put("received", clusterConsumer.received());
        stats.put("skippedSelf", clusterConsumer.skippedSelf());
        stats.put("skippedNoLocalSub", clusterConsumer.skippedNoLocalSub());
        stats.put("uplink", uplinkCount.get());
        // 因广播开关/过滤器而未进消息面的条数。它是「这个开关省下了多少」的唯一量化口径,
        // 也是排查「消息为什么没跨节点」时的第一位数
        stats.put("broadcastSkipped", broadcastSkippedCount.get());
        stats.put("consumerThreads", (long) clusterConsumer.workerCount());
        stats.put("downlinkWorkers", downlinkConsumer == null ? 0L : (long) downlinkConsumer.workerCount());
        // 跨节点接管: applied = 因事件释放的旧连接数; skippedStale = 迟到/不可比而放弃的事件数
        stats.put("takeoverApplied", takeoverHandler == null ? 0L : takeoverHandler.applied());
        stats.put("takeoverSkippedStale", takeoverHandler == null ? 0L : takeoverHandler.skippedStale());
        stats.put("takeoverSkippedNotLocal", takeoverHandler == null ? 0L : takeoverHandler.skippedNotLocal());
        stats.put("outboxSize", producer.outboxSize());
        stats.put("outboxCapacity", producer.outboxCapacity());
        // 事件循环提交了但编码队列已满而丢弃的条数(producer 的 dropped 是出站队列满)
        stats.put("serializeDropped", serializer.dropped());
        return stats;
    }

    // ------------------------------------------------------------------
    // 路由匹配
    // ------------------------------------------------------------------

    /**
     * 单条上行路由的匹配器: 过滤器树 + 目标 topic + 分区键策略。
     * 过滤器列表为空视为「全部命中」(与上行/广播过滤器的语义一致)。
     */
    private static final class RouteMatcher {

        final BrokerProperties.KafkaProperties.Route route;
        private final TopicTrie<Boolean> trie = new TopicTrie<>();
        private final boolean matchAll;

        RouteMatcher(BrokerProperties.KafkaProperties.Route route) {
            this.route = route;
            java.util.List<String> filters = route.filters();
            if (filters == null || filters.isEmpty()) {
                this.matchAll = true;
            } else {
                this.matchAll = false;
                for (String filter : filters) {
                    if (!MqttTopic.isValidFilter(filter)) {
                        log.warn("忽略非法的上行路由过滤器: {}", filter);
                        continue;
                    }
                    trie.insert(MqttTopic.levels(filter), filter, Boolean.TRUE);
                }
            }
        }

        boolean matches(String topic) {
            if (matchAll) {
                return true;
            }
            return topic != null && !topic.isEmpty()
                    && trie.hasMatch(MqttTopic.levels(topic));
        }
    }
}
