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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package online.ipuff.jmqtt.admin;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.mqtt.MqttQoS;
import online.ipuff.jmqtt.cluster.InternalMessage;
import online.ipuff.jmqtt.config.BrokerProperties;
import online.ipuff.jmqtt.redis.RedisConnectionManager;
import online.ipuff.jmqtt.router.MqttTopic;
import online.ipuff.jmqtt.router.TopicTrie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 消息监听(topic 订阅录制): 把匹配某过滤器的消息写入 Redis, 供控制台查看。
 *
 * <h2>两种维度</h2>
 * <ul>
 *   <li><b>topic 维度</b>: 按过滤器抓<b>发布</b>到该主题的消息(挂 PUBLISH 路径)</li>
 *   <li><b>客户端维度</b>: 按 clientId 抓该客户端<b>发布与收到</b>的全部消息 ——
 *       发布侧挂 PUBLISH 路径, 收到侧挂投递循环(InternalSendServer),
 *       消息记录带 {@code direction}(pub/sub)区分方向</li>
 * </ul>
 *
 * <h2>定位: 排障工具, 不是数据通道</h2>
 * 排查「设备到底发了什么」时, 让现场在可疑 topic 上开一个短时监听,
 * 在控制台直接看原始报文 —— 而不用写消费者程序。默认 10 分钟、1000 条,
 * Redis 侧 3 天过期, 用完即弃。
 *
 * <h2>投递路径上的纪律</h2>
 * 挂在 PUBLISH 处理路径上, 因此:
 * <ol>
 *   <li><b>零活跃时零开销</b> —— 一次空表判断直接返回;</li>
 *   <li>命中也<b>只做一次有界队列入队</b>, Redis 写入由专职线程异步完成;
 *       队列满丢弃并计数(监听本身允许有损, 绝不反压 MQTT 投递);</li>
 *   <li>payload 只保留预览(2KB 上限)—— 排障看的是报文形态, 不是搬运数据。</li>
 * </ol>
 *
 * <h2>键布局(与控制台的 AdminKeys 成对)</h2>
 * <pre>
 * {prefix}:admin:captures          SET   全部监听任务 id(注册表)
 * {prefix}:admin:capture:{id}      HASH  任务元数据 + 状态, TTL 3 天
 * {prefix}:admin:capture:{id}:msgs LIST  消息(新的在左), LTRIM 到 maxMessages, TTL 3 天
 * </pre>
 * 状态机: RUNNING → DONE(到时)/ FULL(条数满)/ STOPPED(人工删除)。终态后内存表移除,
 * 不再写入; 数据留给 Redis TTL 自然回收(删除是控制台主动 DEL)。
 */
@Service
public class TopicCaptureService {

    private static final Logger log = LoggerFactory.getLogger(TopicCaptureService.class);

    /** 监听数据的 Redis 保留时长(3 天) —— 排障数据的时效, 不做配置 */
    private static final int RETENTION_SECONDS = 3 * 24 * 3600;

    /** 单条消息的 payload 预览上限 */
    private static final int PAYLOAD_PREVIEW_LIMIT = 2048;

    /** 异步写入队列容量: 满了丢弃(监听允许有损, 投递不允许被拖) */
    private static final int QUEUE_CAPACITY = 4096;

    /** 时长/条数上限: 防止误配成一个「永不停歇的数据搬运」 */
    public static final int MAX_DURATION_MINUTES = 24 * 60;
    public static final int MAX_MESSAGES = 10_000;

    private final ObjectProvider<RedisConnectionManager> redis;
    private final AdminRedisKeys keys;
    private final String nodeId;
    private final ObjectMapper mapper = new ObjectMapper();

    /** 活跃监听表: id → 任务。空表时投递路径零开销 */
    private final ConcurrentHashMap<String, ActiveCapture> active = new ConcurrentHashMap<>();

    /** 匹配树: 过滤器 → captureId。与订阅主题树同一套通配符语义 */
    private final TopicTrie<List<ActiveCapture>> matcher = new TopicTrie<>();

    /** 异步写入队列(在线程外构造, 投递路径只做 offer) */
    private final BlockingQueue<CapturedMessage> outbox = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
    private final AtomicLong droppedCount = new AtomicLong();
    private final Thread worker;

    private record CaptureSpec(String id, String filter, int durationMinutes, int maxMessages) {
    }

    /** 投递路径上缓存的匹配快照: 只有匹配树命中的任务才会真正入队 */
    static final class ActiveCapture {
        final String id;
        final String filter;
        /** 客户端维度目标; null = topic 维度 */
        final String clientId;
        final long endsAt;
        final int maxMessages;
        final AtomicLong captured = new AtomicLong();

        ActiveCapture(String id, String filter, String clientId, long endsAt, int maxMessages) {
            this.id = id;
            this.filter = filter;
            this.clientId = clientId;
            this.endsAt = endsAt;
            this.maxMessages = maxMessages;
        }

        boolean matchesClient(String target) {
            return clientId != null && clientId.equals(target);
        }
    }

    /** 一条被抓到的消息(投递路径构建, 异步线程写 Redis) */
    private record CapturedMessage(String captureId, long timestamp, String topic, int qos,
                                    String clientId, boolean retain, int size, String payloadPreview,
                                    String direction) {
    }

    public TopicCaptureService(ObjectProvider<RedisConnectionManager> redis,
                               AdminRedisKeys keys,
                               BrokerProperties properties) {
        this.redis = redis;
        this.keys = keys;
        this.nodeId = properties.id();
        this.worker = new Thread(this::flushLoop, "jmqtt-capture");
        this.worker.setDaemon(true);
    }

    /** 由 AdminCommandPoller 在首个命令到达时唤醒(避免无 Redis 时空转) */
    private volatile boolean started = false;

    private synchronized void ensureStarted() {
        if (started) {
            return;
        }
        started = true;
        worker.start();
        log.info("消息监听已就绪(首个命令到达时激活)");
    }

    // ------------------------------------------------------------------
    // 投递路径挂点: PublishHandler 每条 PUBLISH 调一次
    // ------------------------------------------------------------------

    /**
     * 发布侧挂点(PublishHandler)。<b>零活跃时只有一次空表判断</b>。
     * topic 维度按过滤器匹配; 客户端维度按发布者 clientId 匹配(direction=pub)。
     */
    public void onPublish(InternalMessage message) {
        if (active.isEmpty()) {
            return;
        }
        if (message.clientId() != null) {
            for (ActiveCapture capture : active.values()) {
                if (capture.matchesClient(message.clientId())) {
                    offer(capture, message, "pub");
                }
            }
        }
        Map<String, List<ActiveCapture>> hits =
                matcher.matchTopic(MqttTopic.levels(message.topic()), null);
        for (List<ActiveCapture> captures : hits.values()) {
            for (ActiveCapture capture : captures) {
                if (capture.clientId != null) {
                    continue; // 客户端维度不走 topic 树(上面已处理)
                }
                offer(capture, message, "pub");
            }
        }
    }

    /**
     * 投递侧挂点(InternalSendServer, 每个收到消息的订阅者调一次)。
     * 只服务客户端维度(direction=sub): 订阅者 clientId 匹配即记录 ——
     * 记录的是「该客户端收到了什么」, 主题/发布者信息随消息带上。
     */
    public void onDeliver(InternalMessage message, String subscriberId) {
        if (active.isEmpty()) {
            return;
        }
        for (ActiveCapture capture : active.values()) {
            if (capture.matchesClient(subscriberId)) {
                offer(capture, message, "sub");
            }
        }
    }

    private void offer(ActiveCapture capture, InternalMessage message, String direction) {
        long count = capture.captured.incrementAndGet();
        if (count > capture.maxMessages) {
            // 超上限: 状态交给后台检查翻成 FULL, 这里直接不再入队
            return;
        }
        byte[] payload = message.payload() == null ? new byte[0] : message.payload();
        boolean queued = outbox.offer(new CapturedMessage(
                capture.id, System.currentTimeMillis(), message.topic(), message.qos(),
                message.clientId(), message.retain(), payload.length,
                preview(payload), direction));
        if (!queued) {
            capture.captured.decrementAndGet();
            long dropped = droppedCount.incrementAndGet();
            if (dropped == 1 || dropped % 1000 == 0) {
                log.warn("监听写入队列已满, 累计丢弃 {} 条(投递不受影响)", dropped);
            }
        }
    }

    /** 单条序列化; 失败丢这一条(返回 null 跳过), 不让一条坏数据废掉整批 */
    private String toJson(CapturedMessage message) {
        try {
            return mapper.writeValueAsString(message);
        } catch (Exception e) {
            log.debug("监听消息序列化失败: {}", e.toString());
            return null;
        }
    }

    private static String preview(byte[] payload) {
        int limit = Math.min(payload.length, PAYLOAD_PREVIEW_LIMIT);
        StringBuilder sb = new StringBuilder(limit + 16);
        for (int i = 0; i < limit; i++) {
            char c = (char) (payload[i] & 0xFF);
            sb.append(c >= 0x20 && c < 0x7F ? c : (c == '\n' || c == '\t' ? ' ' : '?'));
        }
        if (payload.length > limit) {
            sb.append("...(truncated, total=").append(payload.length).append("B)");
        }
        return sb.toString();
    }

    // ------------------------------------------------------------------
    // 异步写线程 + 到期/满员检查
    // ------------------------------------------------------------------

    private void flushLoop() {
        List<CapturedMessage> batch = new ArrayList<>(128);
        while (true) {
            try {
                batch.clear();
                CapturedMessage first = outbox.poll(200, TimeUnit.MILLISECONDS);
                if (first == null) {
                    checkExpiries();
                    continue;
                }
                batch.add(first);
                outbox.drainTo(batch, 127);
                flush(batch);
                checkExpiries();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.warn("监听消息写入失败(批 {} 条, 数据丢失不影响投递): {}", batch.size(), e.toString());
            }
        }
    }

    private void flush(List<CapturedMessage> batch) {
        RedisConnectionManager manager = redis.getIfAvailable();
        if (manager == null) {
            return;
        }
        manager.execute("capture.flush", () -> {
            // 按 captureId 分组, 各自 LPUSH + LTRIM
            Map<String, List<CapturedMessage>> byCapture = new java.util.LinkedHashMap<>();
            for (CapturedMessage message : batch) {
                byCapture.computeIfAbsent(message.captureId(), k -> new ArrayList<>()).add(message);
            }
            for (Map.Entry<String, List<CapturedMessage>> entry : byCapture.entrySet()) {
                String id = entry.getKey();
                String msgsKey = keys.captureMessages(id);
                for (CapturedMessage message : entry.getValue()) {
                    String json = toJson(message);
                    if (json != null) {
                        manager.commands().lpush(msgsKey, json);
                    }
                }
                ActiveCapture capture = active.get(id);
                int max = capture != null ? capture.maxMessages : MAX_MESSAGES;
                manager.commands().ltrim(msgsKey, 0, max - 1L);
                manager.commands().expire(msgsKey, RETENTION_SECONDS);
            }
            return null;
        }, null);
    }

    /** 到期/满员检查: 翻终态、写回元数据、移出内存表(与 flush 同线程, 无并发) */
    private void checkExpiries() {
        if (active.isEmpty()) {
            return;
        }
        long now = System.currentTimeMillis();
        for (ActiveCapture capture : active.values()) {
            String terminal = null;
            if (capture.captured.get() >= capture.maxMessages) {
                terminal = "FULL";
            } else if (now >= capture.endsAt) {
                terminal = "DONE";
            }
            if (terminal != null) {
                finish(capture, terminal);
            }
        }
    }

    private void finish(ActiveCapture capture, String status) {
        active.remove(capture.id);
        removeFromMatcher(capture);
        writeMeta(capture, status);
        log.info("监听任务结束: id={} filter={} 状态={} 已抓={}",
                capture.id, capture.filter, status, capture.captured.get());
    }

    private void removeFromMatcher(ActiveCapture capture) {
        if (capture.clientId != null) {
            return; // 客户端维度不在匹配树里
        }
        List<ActiveCapture> captures = matcher.matchTopic(
                MqttTopic.levels(capture.filter), null).get(capture.filter);
        if (captures != null) {
            captures.remove(capture);
            if (captures.isEmpty()) {
                matcher.remove(MqttTopic.levels(capture.filter), capture.filter);
            }
        }
    }

    // ------------------------------------------------------------------
    // 命令入口(AdminCommandPoller 调用)
    // ------------------------------------------------------------------

    /**
     * 开始一个监听任务。
     *
     * @return 错误信息; null 表示成功
     */
    public String start(String id, String filter, String clientId,
                        int durationMinutes, int maxMessages) {
        if (id == null || id.isBlank()) {
            return "参数非法: id 为空";
        }
        if (clientId == null && (filter == null || !MqttTopic.isValidFilter(filter))) {
            return "参数非法: topic 与 clientId 至少要有一个合法目标";
        }
        if (durationMinutes < 1 || durationMinutes > MAX_DURATION_MINUTES
                || maxMessages < 1 || maxMessages > MAX_MESSAGES) {
            return "参数超限: 时长 1~" + MAX_DURATION_MINUTES + " 分钟, 条数 1~" + MAX_MESSAGES;
        }
        if (active.containsKey(id)) {
            return "任务已存在: " + id;
        }
        ActiveCapture capture = new ActiveCapture(id,
                clientId != null ? "(客户端 " + clientId + " 的全部消息)" : filter,
                clientId,
                System.currentTimeMillis() + durationMinutes * 60_000L, maxMessages);
        active.put(id, capture);
        if (clientId == null) {
            // topic 维度才进匹配树; 客户端维度在 onPublish/onDeliver 里线性比对
            synchronized (this) {
                List<ActiveCapture> captures = matcher.matchTopic(
                        MqttTopic.levels(filter), null).get(filter);
                if (captures == null) {
                    matcher.insert(MqttTopic.levels(filter), filter,
                            new ArrayList<>(List.of(capture)));
                } else {
                    captures.add(capture);
                }
            }
        }
        writeMeta(capture, "RUNNING");
        ensureStarted();
        log.info("监听任务开始: id={} 维度={} filter={} client={} 时长={}min 上限={}条",
                id, clientId != null ? "client" : "topic", filter, clientId,
                durationMinutes, maxMessages);
        return null;
    }

    /** 停止一个监听任务(数据保留, 由控制台删除或 TTL 回收) */
    public String stop(String id) {
        ActiveCapture capture = active.remove(id);
        if (capture == null) {
            return null; // 已结束/不存在, 幂等成功
        }
        removeFromMatcher(capture);
        writeMeta(capture, "STOPPED");
        log.info("监听任务停止: id={} 已抓={}", id, capture.captured.get());
        return null;
    }

    // ------------------------------------------------------------------
    // 元数据
    // ------------------------------------------------------------------

    private void writeMeta(ActiveCapture capture, String status) {
        RedisConnectionManager manager = redis.getIfAvailable();
        if (manager == null) {
            return;
        }
        manager.execute("capture.meta", () -> {
            String key = keys.capture(id0(capture));
            manager.commands().sadd(keys.captures(), id0(capture));
            Map<String, String> meta = new java.util.LinkedHashMap<>();
            meta.put("id", capture.id);
            meta.put("filter", capture.filter);
            meta.put("node", nodeId);
            meta.put("status", status);
            meta.put("captured", String.valueOf(capture.captured.get()));
            meta.put("maxMessages", String.valueOf(capture.maxMessages));
            meta.put("endsAt", String.valueOf(capture.endsAt));
            if (capture.clientId != null) {
                meta.put("mode", "client");
                meta.put("clientId", capture.clientId);
            } else {
                meta.put("mode", "topic");
            }
            manager.commands().hset(key, meta);
            manager.commands().expire(key, RETENTION_SECONDS);
            return null;
        }, null);
    }

    private static String id0(ActiveCapture capture) {
        return capture.id;
    }
}
