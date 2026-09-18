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
import online.ipuff.jmqtt.cluster.InternalMessage;
import online.ipuff.jmqtt.cluster.InternalSendServer;
import online.ipuff.jmqtt.cluster.TakeoverListener;
import online.ipuff.jmqtt.config.BrokerProperties;
import online.ipuff.jmqtt.router.MqttTopic;
import online.ipuff.jmqtt.router.TopicTrie;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 基于 Kafka 的集群总线。
 *
 * <h2>消息面: 每节点独立消费组的广播</h2>
 * 发布时写入 {@code cluster-topic}; 每个节点用<b>各自的 group.id</b> 消费该 topic,
 * 因此每个节点都收到全量消息, 再各自与本地主题树匹配投递。
 *
 * <h2>三条关键实现约定</h2>
 * <ol>
 *   <li><b>跳过自身消息</b> —— {@code brokerId} 等于本节点时直接丢弃。
 *       发布节点在发布瞬间已完成本地投递, 若再从总线取回会重复投递。</li>
 *   <li><b>出站不阻塞</b> —— {@link #publish} 只往有界队列里塞, 立刻返回。
 *       队列满时丢弃并计数。这里的选择是明确的: <b>宁可丢跨节点消息,
 *       也不能让 Kafka 故障卡住本节点投递</b>。</li>
 *   <li><b>ingress 不再出站</b> —— {@link #consumeLoop} 只调用
 *       {@link InternalSendServer#sendPublishMessage}, 不经过任何出站路径,
 *       从结构上排除回环。</li>
 * </ol>
 *
 * <h2>数据面: 上行出口</h2>
 * 命中 {@code uplink-filters} 的消息额外写入 {@code uplink-topic}, 供下游消费。
 * 若 {@code uplink-exclusive=true}, 这类消息<b>只</b>走数据面、不再进集群广播 ——
 * 适用于订阅者集中在云端而非分布在各 broker 的拓扑, 可把扇出从 N 倍降到 1 倍。
 */
@Component
@ConditionalOnProperty(prefix = "jmqtt.broker.kafka", name = "enabled", havingValue = "true")
public class KafkaClusterBus implements ClusterBus, ClusterBusStats, SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(KafkaClusterBus.class);

    /** 单次批量取出的最大记录数 */
    private static final int SEND_BATCH_SIZE = 500;

    private final BrokerProperties properties;
    private final InternalSendServer internalSendServer;
    private final TakeoverListener takeoverListener;

    /** 出站队列: publish() 只做入队, 由独立线程消费 */
    private final BlockingQueue<ProducerRecord<String, byte[]>> outbox;

    /** 上行过滤器匹配树; 过滤器为空时 uplinkAll = true */
    private final TopicTrie<Boolean> uplinkTrie = new TopicTrie<>();
    private volatile boolean uplinkAll;

    /**
     * 广播过滤器匹配树。与上行过滤器共用同一套通配符语义(而不是字符串前缀判断) ——
     * 配 {@code device/+/telemetry} 就应该真的按 MQTT 通配符匹配,
     * 否则「配置写的」和「实际执行的」是两回事。
     */
    private final TopicTrie<Boolean> broadcastTrie = new TopicTrie<>();
    private volatile boolean broadcastAll = true;

    private volatile KafkaProducer<String, byte[]> producer;
    private volatile KafkaConsumer<String, byte[]> consumer;
    private volatile Thread senderThread;
    private volatile Thread consumerThread;
    private volatile boolean running;

    private final AtomicLong publishedCount = new AtomicLong();
    private final AtomicLong droppedCount = new AtomicLong();
    private final AtomicLong receivedCount = new AtomicLong();
    private final AtomicLong skippedSelfCount = new AtomicLong();
    private final AtomicLong uplinkCount = new AtomicLong();
    private final AtomicLong takeoverAppliedCount = new AtomicLong();
    private final AtomicLong broadcastSkippedCount = new AtomicLong();

    public KafkaClusterBus(BrokerProperties properties,
                           InternalSendServer internalSendServer,
                           TakeoverListener takeoverListener) {
        this.properties = properties;
        this.internalSendServer = internalSendServer;
        this.takeoverListener = takeoverListener;
        this.outbox = new ArrayBlockingQueue<>(properties.kafka().queueCapacity());
        initUplinkMatcher();
        initBroadcastMatcher();
    }

    /**
     * 初始化广播过滤器。
     *
     * <p>三种形态要严格区分开, 因为它们对应完全不同的业务前提:
     * <ol>
     *   <li><b>关闭广播</b> —— 业务上不存在跨节点 MQTT 订阅, 消息只在本节点投递 + 走上行数据面。</li>
     *   <li><b>全部广播</b>(未配置过滤器) —— 默认行为, 保持既有语义不变。</li>
     *   <li><b>按过滤器广播</b> —— 只有可能被其他节点订阅的主题才跨节点, 例如端与端之间的信令。</li>
     * </ol>
     * 第 2、3 种都靠「过滤器列表是否为空」区分, 所以第 1 种必须由独立的开关表达 ——
     * 不能复用「空列表」, 否则「关闭广播」与「配置漏了」会变成同一个状态。
     */
    private void initBroadcastMatcher() {
        BrokerProperties.KafkaProperties kafka = properties.kafka();
        if (!kafka.broadcastEnabled()) {
            log.warn("集群广播已关闭(broadcast-enabled=false)。本节点投递与数据面上行不受影响, "
                    + "但其他节点上的 MQTT 订阅者收不到本节点发布的消息。"
                    + "仅在确认业务上不存在跨节点 MQTT 订阅时使用 —— 这个开关一旦配错, "
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
        // 分区键: 数据面默认按主题, 可按配置改为按设备 —— 后者才有「每个设备自身有序 + 流量散开」
        String partitionKey = properties.kafka().uplinkPartitionKey(message.clientId(), message.topic());
        enqueue(ClusterRecords.toRecord(topic, partitionKey, message), message.topic());
    }

    @Override
    public boolean uplinkEnabled() {
        return properties.kafka().uplinkEnabled();
    }

    @Override
    public boolean isUplink(String topic) {
        if (!uplinkEnabled() || topic == null || topic.isEmpty()) {
            return false;
        }
        if (uplinkAll) {
            return true;
        }
        return !uplinkTrie.matchTopic(MqttTopic.levels(topic), null).isEmpty();
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
        boolean allowed = !broadcastTrie.matchTopic(MqttTopic.levels(topic), null).isEmpty();
        if (!allowed) {
            // 计进指标而不是打日志: 关掉广播之后每一批遥测都会走到这里,
            // 打日志等于用日志把磁盘写满
            broadcastSkippedCount.incrementAndGet();
        }
        return allowed;
    }

    @Override
    public void publishUplink(InternalMessage message) {
        uplinkCount.incrementAndGet();
        String topic = properties.kafka().uplinkTopic();
        if (topic == null || topic.isEmpty()) {
            return;
        }
        enqueue(ClusterRecords.toRecord(topic, message), message.topic());
    }

    @Override
    public void publishTakeover(String clientId, String targetNodeId) {
        String topic = properties.kafka().clusterTopic();
        enqueue(ClusterRecords.takeoverRecord(topic,
                new ClusterRecords.Takeover(clientId, targetNodeId, properties.id())), clientId);
    }

    private void enqueue(ProducerRecord<String, byte[]> record, String description) {
        if (outbox.offer(record)) {
            publishedCount.incrementAndGet();
            return;
        }
        // 队列已满: 丢弃优先于阻塞。跨节点投递是尽力而为, 本节点投递必须畅通。
        long dropped = droppedCount.incrementAndGet();
        if (dropped == 1 || dropped % 1000 == 0) {
            log.warn("Kafka 出站队列已满, 已丢弃 {} 条消息。上游可能过载或 Kafka 不可用",
                    dropped);
        }
        if (log.isDebugEnabled()) {
            log.debug("丢弃消息: {}", description);
        }
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
            this.producer = new KafkaProducer<>(producerConfig(kafka, brokerId));
            this.consumer = new KafkaConsumer<>(consumerConfig(kafka, brokerId));
        } catch (KafkaException e) {
            // 启动期 Kafka 不可用不应导致 broker 起不来, 但必须让运维明确知道
            log.error("Kafka 客户端初始化失败, 集群功能不可用。本地投递不受影响", e);
            closeQuietly();
            return;
        }

        consumer.subscribe(List.of(kafka.clusterTopic()));
        this.running = true;

        this.senderThread = new Thread(this::sendLoop, "jmqtt-kafka-sender");
        this.senderThread.setDaemon(true);
        this.senderThread.start();

        this.consumerThread = new Thread(this::consumeLoop, "jmqtt-kafka-consumer");
        this.consumerThread.setDaemon(true);
        this.consumerThread.start();

        log.info("Kafka 集群总线已启动: clusterTopic={} groupId={} uplinkTopic={} uplinkExclusive={}",
                kafka.clusterTopic(), kafka.resolvedGroupId(brokerId),
                kafka.uplinkEnabled() ? kafka.uplinkTopic() : "(未启用)",
                kafka.uplinkExclusive());
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

        if (consumer != null) {
            consumer.wakeup();
        }
        joinQuietly(consumerThread);
        joinQuietly(senderThread);

        // 停止前尽力把队列里剩下的消息发出去
        try {
            if (producer != null) {
                producer.flush();
            }
        } catch (Exception e) {
            log.warn("关闭前 flush 失败", e);
        }
        closeQuietly();
        log.info("Kafka 集群总线已停止.");
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
    // 发送线程
    // ------------------------------------------------------------------

    private void sendLoop() {
        List<ProducerRecord<String, byte[]>> batch = new ArrayList<>(SEND_BATCH_SIZE);
        while (running) {
            try {
                batch.clear();
                int drained = outbox.drainTo(batch, SEND_BATCH_SIZE);
                if (drained == 0) {
                    ProducerRecord<String, byte[]> single = outbox.poll(200, TimeUnit.MILLISECONDS);
                    if (single == null) {
                        continue;
                    }
                    batch.add(single);
                }
                KafkaProducer<String, byte[]> p = producer;
                if (p == null) {
                    continue;
                }
                // send() 是异步的, 这里连续调用让生产者在 linger 窗口内自行攒批
                for (ProducerRecord<String, byte[]> record : batch) {
                    p.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            log.debug("Kafka 发送失败 topic={}", record.topic(), exception);
                        }
                    });
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.warn("Kafka 发送线程异常, 继续运行", e);
            }
        }
    }

    // ------------------------------------------------------------------
    // 消费线程
    // ------------------------------------------------------------------

    private void consumeLoop() {
        KafkaConsumer<String, byte[]> c = consumer;
        if (c == null) {
            return;
        }
        Duration pollTimeout = Duration.ofMillis(properties.kafka().pollTimeoutMs());
        try {
            while (running) {
                ConsumerRecords<String, byte[]> records = c.poll(pollTimeout);
                if (records.isEmpty()) {
                    continue;
                }
                for (ConsumerRecord<String, byte[]> record : records) {
                    handle(record);
                }
                // 手动提交: 处理完再提交, 保证至少一次
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                records.partitions().forEach(tp ->
                        offsets.put(tp, new OffsetAndMetadata(c.position(tp))));
                c.commitSync(offsets);
            }
        } catch (WakeupException e) {
            if (running) {
                log.warn("Kafka 消费线程被意外唤醒", e);
            }
        } catch (Exception e) {
            log.error("Kafka 消费线程异常退出, 集群消息将不再被接收", e);
        } finally {
            closeQuietly();
        }
    }

    private void handle(ConsumerRecord<String, byte[]> record) {
        // 同一条 topic 上混跑两类记录: 普通消息与控制指令
        if (ClusterRecords.kind(record) == ClusterRecords.Kind.TAKEOVER) {
            handleTakeover(record);
            return;
        }
        handlePublish(record);
    }

    /**
     * 处理连接接管指令。
     *
     * <p>这是<b>定向</b>消息: 只有目标节点才执行, 其他节点直接跳过 ——
     * 否则所有节点都会去关一个自己并没有的连接(虽然是无害的 no-op, 但日志会很难看)。
     */
    private void handleTakeover(ConsumerRecord<String, byte[]> record) {
        ClusterRecords.Takeover takeover = ClusterRecords.takeover(record);
        if (takeover == null) {
            log.debug("丢弃无法解码的接管指令, offset={}", record.offset());
            return;
        }
        if (!properties.id().equals(takeover.targetNodeId())) {
            // 不是发给本节点的, 静默跳过
            return;
        }
        takeoverAppliedCount.incrementAndGet();
        log.info("处理跨节点接管指令: clientId={} 接管方={}", takeover.clientId(), takeover.fromNodeId());
        // 与普通消息一样, 这里只做本节点处理, 绝不回写出站(无回环风险)
        takeoverListener.onTakeover(takeover.clientId(), takeover.fromNodeId());
    }

    private void handlePublish(ConsumerRecord<String, byte[]> record) {
        InternalMessage message = ClusterRecords.fromRecord(record);
        if (message == null) {
            log.debug("丢弃无法解码的集群记录, offset={}", record.offset());
            return;
        }
        // 关键: 跳过本节点自己发出的消息。
        // 发布路径已经完成过一次本地投递, 再取回来会重复投递。
        if (properties.id().equals(message.brokerId())) {
            skippedSelfCount.incrementAndGet();
            return;
        }
        receivedCount.incrementAndGet();
        // 只做本节点投递。绝不调用出站路径 —— 这是回环防护的结构性保证。
        int delivered = internalSendServer.sendPublishMessage(message);
        if (log.isDebugEnabled()) {
            log.debug("总线消息投递: from={} topic={} 本节点投递={}",
                    message.brokerId(), message.topic(), delivered);
        }
    }

    // ------------------------------------------------------------------
    // 配置
    // ------------------------------------------------------------------

    private Map<String, Object> producerConfig(BrokerProperties.KafkaProperties kafka, String brokerId) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers());
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        cfg.put(ProducerConfig.CLIENT_ID_CONFIG, prefix(kafka) + "-" + brokerId + "-producer");
        cfg.put(ProducerConfig.ACKS_CONFIG, "1");
        cfg.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        cfg.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        cfg.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, kafka.compressionType());
        // 关键: 元数据不可用时的最长阻塞。默认 60s 会直接卡住 MQTT 发布路径
        cfg.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, kafka.maxBlockMs());
        cfg.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000);
        cfg.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
        return cfg;
    }

    private Map<String, Object> consumerConfig(BrokerProperties.KafkaProperties kafka, String brokerId) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers());
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        // 每节点独立 group.id —— 这是广播语义的来源
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG, kafka.resolvedGroupId(brokerId));
        cfg.put(ConsumerConfig.CLIENT_ID_CONFIG, prefix(kafka) + "-" + brokerId + "-consumer");
        // 手动提交, 保证处理后才推进 offset
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafka.autoOffsetReset());
        cfg.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        cfg.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15_000);
        return cfg;
    }

    private static String prefix(BrokerProperties.KafkaProperties kafka) {
        return (kafka.clientIdPrefix() == null || kafka.clientIdPrefix().isBlank())
                ? "jmqtt" : kafka.clientIdPrefix();
    }

    private void closeQuietly() {
        KafkaProducer<String, byte[]> p = producer;
        producer = null;
        if (p != null) {
            try {
                p.close(Duration.ofSeconds(3));
            } catch (Exception e) {
                log.debug("关闭 producer 异常", e);
            }
        }
        KafkaConsumer<String, byte[]> c = consumer;
        consumer = null;
        if (c != null) {
            try {
                c.close(Duration.ofSeconds(3));
            } catch (Exception e) {
                log.debug("关闭 consumer 异常", e);
            }
        }
    }

    private static void joinQuietly(Thread thread) {
        if (thread == null) {
            return;
        }
        try {
            thread.join(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ------------------------------------------------------------------
    // 指标
    // ------------------------------------------------------------------

    @Override
    public Map<String, Long> stats() {
        Map<String, Long> stats = new LinkedHashMap<>();
        stats.put("published", publishedCount.get());
        stats.put("dropped", droppedCount.get());
        stats.put("received", receivedCount.get());
        stats.put("skippedSelf", skippedSelfCount.get());
        stats.put("uplink", uplinkCount.get());
        stats.put("takeoverApplied", takeoverAppliedCount.get());
        // 因广播开关/过滤器而未进消息面的条数。它是「这个开关省下了多少」的唯一量化口径,
        // 也是排查「消息为什么没跨节点」时的第一位数
        stats.put("broadcastSkipped", broadcastSkippedCount.get());
        stats.put("outboxSize", (long) outbox.size());
        stats.put("outboxCapacity", (long) outbox.size() + outbox.remainingCapacity());
        return stats;
    }
}
