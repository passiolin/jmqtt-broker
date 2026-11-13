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

import online.ipuff.jmqtt.config.BrokerProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka 生产者: 有界出站队列 + 独立发送线程。
 *
 * <p>入队永不阻塞发布路径; 队列满时丢弃并计数 —— 跨节点/上行投递是尽力而为,
 * 本节点投递必须畅通。send() 异步, 攒批交给 producer 的 linger 窗口。
 */
final class KafkaMessageProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaMessageProducer.class);

    /** 单次批量取出的最大记录数 */
    private static final int SEND_BATCH_SIZE = 500;

    private final BrokerProperties properties;

    /** 出站队列: 包可见, 同包测试校验出站 record 结构 */
    final BlockingQueue<ProducerRecord<String, byte[]>> outbox;

    private volatile KafkaProducer<String, byte[]> producer;
    private volatile Thread senderThread;
    private volatile boolean running;

    private final AtomicLong publishedCount = new AtomicLong();
    private final AtomicLong droppedCount = new AtomicLong();

    KafkaMessageProducer(BrokerProperties properties) {
        this.properties = properties;
        this.outbox = new ArrayBlockingQueue<>(properties.kafka().queueCapacity());
    }

    void start() {
        this.producer = new KafkaProducer<>(config());
        this.running = true;
        this.senderThread = new Thread(this::sendLoop, "jmqtt-kafka-sender");
        this.senderThread.setDaemon(true);
        this.senderThread.start();
    }

    void stop() {
        if (!running) {
            return;
        }
        running = false;
        joinSender();
        // 停止前尽力把队列里剩下的消息发出去
        try {
            KafkaProducer<String, byte[]> p = producer;
            if (p != null) {
                p.flush();
            }
        } catch (Exception e) {
            log.warn("关闭前 flush 失败", e);
        }
        closeQuietly();
    }

    boolean isRunning() {
        return running;
    }

    /**
     * 入队一条出站记录; 不阻塞。
     *
     * <p>队列满时丢弃优先于阻塞: 跨节点投递是尽力而为, 本节点投递必须畅通。
     */
    void enqueue(ProducerRecord<String, byte[]> record, String description) {
        if (outbox.offer(record)) {
            publishedCount.incrementAndGet();
            return;
        }
        long dropped = droppedCount.incrementAndGet();
        if (dropped == 1 || dropped % 1000 == 0) {
            log.warn("Kafka 出站队列已满, 已丢弃 {} 条消息。上游可能过载或 Kafka 不可用",
                    dropped);
        }
        if (log.isDebugEnabled()) {
            log.debug("丢弃消息: {}", description);
        }
    }

    long published() {
        return publishedCount.get();
    }

    long dropped() {
        return droppedCount.get();
    }

    long outboxSize() {
        return outbox.size();
    }

    long outboxCapacity() {
        return outbox.size() + outbox.remainingCapacity();
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

    private void joinSender() {
        Thread t = senderThread;
        senderThread = null;
        if (t == null) {
            return;
        }
        try {
            t.join(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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
    }

    private Map<String, Object> config() {
        BrokerProperties.KafkaProperties kafka = properties.kafka();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers());
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        cfg.put(ProducerConfig.CLIENT_ID_CONFIG, prefix(kafka) + "-" + properties.id() + "-producer");
        // acks=0 + 大批(linger 50): 压测定稿组合(2026-11 A/B)。
        // 大批把请求数降 ~20 倍后, 不等响应的收益才盖过高频小请求打爆 Kafka 的代价 ——
        // 同负载下 broker CPU 更低、outbox 丢弃更少、重发乱序重复消失。
        // 代价是零持久性: Kafka 故障/超时的在途批次无声消失, 无重试无日志 ——
        // 遥测数据面可接受; 控制面消息若对此敏感, 需重估此值
        cfg.put(ProducerConfig.ACKS_CONFIG, "0");
        cfg.put(ProducerConfig.LINGER_MS_CONFIG, 50);
        cfg.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        cfg.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, kafka.compressionType());
        // 关键: 元数据不可用时的最长阻塞。默认 60s 会直接卡住 MQTT 发布路径
        cfg.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, kafka.maxBlockMs());
        cfg.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000);
        cfg.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
        return cfg;
    }

    static String prefix(BrokerProperties.KafkaProperties kafka) {
        return (kafka.clientIdPrefix() == null || kafka.clientIdPrefix().isBlank())
                ? "jmqtt" : kafka.clientIdPrefix();
    }
}
