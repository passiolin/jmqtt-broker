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
package online.ipuff.jmqtt.cluster.kafka;

import online.ipuff.jmqtt.cluster.ConnectionEvent;
import online.ipuff.jmqtt.cluster.InternalMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka 记录的异步编码器: 把 JSON + base64 从 MQTT event loop 上挪走。
 *
 * <h2>为什么需要</h2>
 * 上行每条消息都要做一次 Jackson 序列化 + base64(payload 是 byte[]),
 * 这个 CPU 成本原本落在发布者的 event loop 上。连接数上来以后,
 * event loop 既要跑 MQTT 编解码又要序列化 Kafka 记录, 互相拖尾。
 *
 * <h2>为什么按 key 分片, 而不是随便多个线程</h2>
 * Kafka 的顺序保证是「同 key 分区内有序」。如果同一个 key 的两条消息被两个线程
 * 并发编码、再以任意顺序进 outbox, 顺序就断了。这里用 {@code hash(key) % shards}
 * 把同一 key 固定到同一线程: 单线程 FIFO 编码 + FIFO 入 outbox ⇒ 同 key 有序;
 * 不同 key 并行, 吞吐不受单线程限制。
 *
 * <h2>满了怎么办</h2>
 * 和 producer 出站队列同一口径: <b>丢弃并计数, 绝不阻塞 event loop</b>。
 * 丢一条上行或连接事件的代价远小于让发布路径卡住。
 */
final class KafkaRecordSerializer {

    private static final Logger log = LoggerFactory.getLogger(KafkaRecordSerializer.class);

    /** 分片数: 8C 下 2 个足够(单条 256B 的 JSON+base64 只有几微秒), 核多时也不无限放大 */
    private static final int DEFAULT_SHARDS = Math.max(1, Runtime.getRuntime().availableProcessors() / 4);

    /** 每个分片的待编码队列上限。与 producer outbox 一样, 只做缓冲不做无界堆积 */
    private static final int SHARD_CAPACITY = 20_000;

    private final KafkaMessageProducer producer;
    private final Shard[] shards;
    private final AtomicLong dropped = new AtomicLong();
    private volatile boolean running;

    KafkaRecordSerializer(KafkaMessageProducer producer) {
        this(producer, DEFAULT_SHARDS);
    }

    KafkaRecordSerializer(KafkaMessageProducer producer, int shardCount) {
        this.producer = producer;
        int count = Math.max(1, shardCount);
        this.shards = new Shard[count];
        for (int i = 0; i < count; i++) {
            shards[i] = new Shard("jmqtt-kafka-serialize-" + i);
        }
    }

    void start() {
        running = true;
        for (Shard shard : shards) {
            shard.start();
        }
    }

    void stop() {
        running = false;
        for (Shard shard : shards) {
            shard.stop();
        }
    }

    /** 集群广播: key = MQTT 主题。 */
    boolean publishBroadcast(String kafkaTopic, InternalMessage message) {
        return submit(new Task(kafkaTopic, null, message, null,
                System.currentTimeMillis(), message.topic()));
    }

    /** 数据面上行: key 由路由决定(topic 或设备标识)。 */
    boolean publishRouted(String kafkaTopic, String partitionKey, InternalMessage message) {
        return submit(new Task(kafkaTopic, partitionKey, message, null,
                System.currentTimeMillis(), message.topic()));
    }

    /** 连接事件: key = clientId。 */
    boolean publishConnectionEvent(String kafkaTopic, ConnectionEvent event) {
        return submit(new Task(kafkaTopic, event.clientid(), null, event,
                System.currentTimeMillis(), "connection-event:" + event.action() + ":" + event.clientid()));
    }

    long dropped() {
        return dropped.get();
    }

    /** 测试用: 等所有已提交任务编码完成(阻塞到队列排空)。 */
    void flush() throws InterruptedException {
        if (!running) {
            return;
        }
        for (Shard shard : shards) {
            shard.flush();
        }
    }

    private boolean submit(Task task) {
        if (!running) {
            // 启动/停止窗口: 没有后台线程可等, 同步编码比静默丢弃安全
            serialize(task);
            return true;
        }
        if (!shardFor(task).offer(task)) {
            long total = dropped.incrementAndGet();
            if (total == 1 || total % 10_000 == 0) {
                log.warn("Kafka 记录编码队列已满, 已丢弃 {} 条。发布路径不受影响, "
                        + "上游可能过载或编码线程跟不上", total);
            }
            return false;
        }
        return true;
    }

    private Shard shardFor(Task task) {
        String key = task.shardKey();
        int hash = key == null ? 0 : key.hashCode();
        return shards[Math.floorMod(hash, shards.length)];
    }

    private void serialize(Task task) {
        if (task.event != null) {
            producer.enqueue(ClusterRecords.connectionEventRecord(task.kafkaTopic, task.event),
                    task.description);
            return;
        }
        if (task.partitionKey == null) {
            producer.enqueue(ClusterRecords.toRecord(task.kafkaTopic, task.timestamp, task.message),
                    task.description);
        } else {
            producer.enqueue(ClusterRecords.toRoutedRecord(
                    task.kafkaTopic, task.partitionKey, task.timestamp, task.message), task.description);
        }
    }

    /** 一条待编码记录。timestamp 在提交时取, 不受编码排队延迟影响。 */
    private static final class Task {

        final String kafkaTopic;
        final String partitionKey;
        final InternalMessage message;
        final ConnectionEvent event;
        final long timestamp;
        final String description;
        /** 非 null 表示 flush 屏障, 不参与编码 */
        final CountDownLatch done;

        Task(String kafkaTopic, String partitionKey, InternalMessage message,
             ConnectionEvent event, long timestamp, String description) {
            this(kafkaTopic, partitionKey, message, event, timestamp, description, null);
        }

        Task(String kafkaTopic, String partitionKey, InternalMessage message,
             ConnectionEvent event, long timestamp, String description, CountDownLatch done) {
            this.kafkaTopic = kafkaTopic;
            this.partitionKey = partitionKey;
            this.message = message;
            this.event = event;
            this.timestamp = timestamp;
            this.description = description;
            this.done = done;
        }

        String shardKey() {
            if (event != null) {
                return event.clientid();
            }
            return partitionKey != null ? partitionKey : message.topic();
        }
    }

    /** 单个编码线程 + 有界队列。 */
    private final class Shard {

        private final BlockingQueue<Task> queue = new ArrayBlockingQueue<>(SHARD_CAPACITY);
        private final Thread thread;
        private volatile boolean stopped;

        Shard(String name) {
            this.thread = new Thread(this::loop, name);
            this.thread.setDaemon(true);
        }

        void start() {
            thread.start();
        }

        boolean offer(Task task) {
            return queue.offer(task);
        }

        private void loop() {
            while (!stopped || !queue.isEmpty()) {
                try {
                    Task task = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (task == null) {
                        continue;
                    }
                    if (task.done != null) {
                        task.done.countDown();
                        continue;
                    }
                    serialize(task);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Exception e) {
                    // 编码失败不能打死线程: 后续消息还要继续走
                    log.warn("Kafka 记录编码失败, 跳过该条", e);
                }
            }
        }

        void stop() {
            stopped = true;
            try {
                thread.join(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        void flush() throws InterruptedException {
            CountDownLatch latch = new CountDownLatch(1);
            Task barrier = new Task("", null, null, null, 0L, "flush", latch);
            while (!queue.offer(barrier, 100, TimeUnit.MILLISECONDS)) {
                // 队列满说明编码线程还在工作, 继续等
            }
            latch.await(5, TimeUnit.SECONDS);
        }
    }
}
