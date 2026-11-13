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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 分区并行消费者的公共骨架。集群广播消费与后台下行消费共享同一套结构:
 *
 * <ul>
 *   <li><b>poll 线程只做 poll / 分派 / 提交</b>, 记录处理全在 worker 上</li>
 *   <li><b>共享队列 + worker 争抢</b>: 并行度只由 worker 数决定, 与 topic 分区数无关;
 *       同分区内不再保证顺序(业务明确不需要); 队列满时背压 poller</li>
 *   <li><b>位点只提交「已连续完成」的部分</b>: 至少一次, 宕机重放最多重复、绝不跳过</li>
 *   <li><b>消费线程不许死</b>: 异常退避后重建 consumer 继续, 不静默停摆</li>
 * </ul>
 *
 * 子类只决定: 订阅哪些 topic、group/clientId 后缀、线程命名、以及单条记录怎么处理。
 */
abstract class PartitionedConsumer {

    private static final Logger log = LoggerFactory.getLogger(PartitionedConsumer.class);

    /** 单个消费 worker 的待处理队列容量: 队列满时 poller 阻塞(背压), 而不是丢弃 */
    protected static final int WORKER_QUEUE_CAPACITY = 1000;

    /** 消费循环异常后重建 consumer 的退避时长 */
    protected static final long RESTART_BACKOFF_MS = 5_000;

    protected final BrokerProperties properties;

    protected volatile boolean running;

    private volatile KafkaConsumer<String, byte[]> consumer;
    private volatile Thread pollerThread;
    /** 消费 worker 池: 所有 worker 从同一条共享队列争抢任务, 并行度与分区数解耦 */
    private volatile Worker[] workers;
    /** 共享任务队列: poller 入队, 任意 worker 出队处理。容量按 worker 数放大以保背压缓冲 */
    private final BlockingQueue<Runnable> workQueue;
    /** 分区完成位点跟踪: 并行消费下「至少一次」的提交依据 */
    private volatile PartitionProgressTracker tracker;


    protected PartitionedConsumer(BrokerProperties properties) {
        this.properties = properties;
        this.workQueue = new ArrayBlockingQueue<>(
                WORKER_QUEUE_CAPACITY * Math.max(1, properties.kafka().consumerThreadsOrDefault()));
    }

    protected abstract String threadName();

    protected abstract String workerNamePrefix();

    /** group.id 后缀: 广播为空(每节点独立 group), 下行 "-downlink" */
    protected abstract String groupIdSuffix();

    /** client.id 后缀: "-consumer" / "-downlink" */
    protected abstract String clientIdSuffix();

    protected abstract List<String> topics();

    /** 单条记录的处理(在分区对应的 worker 线程上执行)。 */
    protected abstract void handle(ConsumerRecord<String, byte[]> record);

    /**
     * 消费 worker 数。默认 2 × 核数; 连接事件这类低频、可延迟的流可以覆盖为 1,
     * 避免为一个每秒几条的 topic 拉起一整个线程池。
     */
    protected int workerPoolSize() {
        return properties.kafka().consumerThreadsOrDefault();
    }

    /**
     * 无已提交位点时的起点。默认跟随 {@code kafka.auto-offset-reset}。
     * 连接事件消费强制 latest: 回放历史事件没有意义且会误触发生过期的接管判断。
     */
    protected String autoOffsetReset() {
        return properties.kafka().autoOffsetReset();
    }

    final void start() {
        this.consumer = new KafkaConsumer<>(config());
        subscribe(this.consumer);
        this.tracker = new PartitionProgressTracker();
        this.running = true;

        int count = Math.max(1, workerPoolSize());
        this.workers = new Worker[count];
        for (int i = 0; i < count; i++) {
            this.workers[i] = new Worker(workerNamePrefix() + i);
            this.workers[i].start();
        }

        this.pollerThread = new Thread(this::pollLoop, threadName());
        this.pollerThread.setDaemon(true);
        this.pollerThread.start();
    }

    final void stop() {
        if (!running) {
            return;
        }
        running = false;
        KafkaConsumer<String, byte[]> c = consumer;
        if (c != null) {
            try {
                c.wakeup();
            } catch (Exception e) {
                log.debug("wakeup consumer 异常", e);
            }
        }
        Thread t = pollerThread;
        if (t != null) {
            try {
                t.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // join 超时(排干耗时)时的兜底: 显式等 worker 收尾, 再关客户端
        drainWorkers();
        closeConsumerQuietly();
    }

    final int workerCount() {
        Worker[] pool = workers;
        return pool == null ? 0 : pool.length;
    }

    // ------------------------------------------------------------------
    // poll 循环
    // ------------------------------------------------------------------

    /**
     * 消费线程(poller): poll → 按分区分派 → 提交位点。
     *
     * <p><b>线程不许死。</b>任何异常只中断「这一轮消费」: 退避后重建 consumer 继续。
     * 旧实现 catch 一下就退出线程, 而节点对外服务一切正常 —— 消费停摆只有一行 ERROR,
     * 直到进程重启才恢复。消费循环必须自我修复。
     */
    private void pollLoop() {
        try {
            while (running) {
                try {
                    pollLoopOnce();
                    return; // running=false 的正常退出
                } catch (WakeupException e) {
                    if (!running) {
                        return;
                    }
                    // 意外唤醒: consumer 本身没坏, 继续用
                    log.warn("Kafka 消费线程被意外唤醒({}), 继续消费", threadName(), e);
                } catch (Throwable t) {
                    if (!running) {
                        return;
                    }
                    log.error("Kafka 消费循环异常({}), {}ms 后重建 consumer 重试(本地投递不受影响)",
                            threadName(), RESTART_BACKOFF_MS, t);
                }
                // 走到这里 = 异常退出: KafkaConsumer 出错后不可复用, 换新的再战
                restartConsumer();
            }
        } finally {
            // 停止顺序: 先排干 worker(在途消息处理完) → 同步提交最后一批位点 → 关客户端。
            // 都在本线程执行 —— KafkaConsumer 只允许被所属线程访问。
            drainWorkers();
            commitProgressQuietly();
            closeConsumerQuietly();
        }
    }

    /** 一轮消费: poll → 分派 → 提交, 直到 {@code running=false}。 */
    private void pollLoopOnce() {
        KafkaConsumer<String, byte[]> c = consumer;
        if (c == null) {
            return;
        }
        Duration pollTimeout = Duration.ofMillis(properties.kafka().pollTimeoutMs());
        while (running) {
            ConsumerRecords<String, byte[]> records = c.poll(pollTimeout);
            if (!records.isEmpty()) {
                dispatch(records);
                // 异步提交已完成位点; 失败无害 —— 下一次 drain 会带上更大的位点重试
                commitProgress(false);
            }
        }
    }

    /**
     * 异常后重建 consumer: 关旧 → 退避 → 建新 → 重新订阅。
     * 位点状态一并重置, 从上次提交处重新拉 —— 至少一次语义下最多重复, 不会跳过。
     */
    private void restartConsumer() {
        closeConsumerQuietly();
        while (running) {
            try {
                this.tracker = new PartitionProgressTracker();
                KafkaConsumer<String, byte[]> fresh = new KafkaConsumer<>(config());
                subscribe(fresh);
                this.consumer = fresh;
                log.info("Kafka consumer 已重建: {}", threadName());
                return;
            } catch (KafkaException e) {
                log.error("Kafka consumer 重建失败, {}ms 后重试", RESTART_BACKOFF_MS, e);
                if (!sleepQuietly(RESTART_BACKOFF_MS)) {
                    return;
                }
            }
        }
    }

    private void subscribe(KafkaConsumer<String, byte[]> c) {
        c.subscribe(topics(), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // 分区被撤销时: 先把已完成部分同步提交, 再丢弃该分区的位点状态
                commitProgress(true);
                PartitionProgressTracker progress = tracker;
                if (progress != null) {
                    progress.clear(partitions);
                }
                if (!partitions.isEmpty()) {
                    log.info("分区被撤销({}), 已提交完成位点并重置跟踪: {}", threadName(), partitions);
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // 位点由已提交 offset / auto-offset-reset 决定, 无需处理
            }
        });
    }

    private void closeConsumerQuietly() {
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

    private static boolean sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    // ------------------------------------------------------------------
    // 分派与位点
    // ------------------------------------------------------------------

    /**
     * 记录入共享队列, 由任意空闲 worker 争抢处理 —— 并行度与 topic 分区数解耦。
     * 同分区内不保证顺序(业务明确不需要)。队列满时阻塞 poller(背压), 不丢弃。
     */
    private void dispatch(ConsumerRecords<String, byte[]> records) {
        if (workers == null || tracker == null) {
            return;
        }
        PartitionProgressTracker progress = tracker;
        for (ConsumerRecord<String, byte[]> record : records) {
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            long offset = record.offset();
            progress.submitted(tp, offset);
            try {
                workQueue.put(() -> {
                    try {
                        handle(record);
                    } catch (Exception e) {
                        // worker 不能因单条坏记录退出; 位点照常推进(跳过), 与串行时代的容错一致
                        log.warn("消费记录处理异常, 跳过 topic={} partition={} offset={}",
                                record.topic(), record.partition(), record.offset(), e);
                    } finally {
                        progress.completed(tp, offset);
                    }
                });
            } catch (InterruptedException e) {
                // 停止中: 未处理也标记完成, 避免该分区位点永久卡住
                Thread.currentThread().interrupt();
                progress.completed(tp, offset);
                return;
            }
        }
    }

    /**
     * 提交「已连续完成」的位点。至少一次语义: 提交只领先于完成部分,
     * 宕机重放最多重复投递已处理消息, 绝不跳过未处理的。
     * 只允许在 poll 线程调用(poll 循环 / rebalance 回调 / 停止序列)。
     */
    private void commitProgress(boolean sync) {
        KafkaConsumer<String, byte[]> c = consumer;
        PartitionProgressTracker progress = tracker;
        if (c == null || progress == null) {
            return;
        }
        Map<TopicPartition, OffsetAndMetadata> offsets = progress.drainCommittable();
        if (offsets.isEmpty()) {
            return;
        }
        if (sync) {
            c.commitSync(offsets);
        } else {
            c.commitAsync(offsets, (off, ex) -> {
                if (ex != null) {
                    log.debug("位点异步提交失败, 将随下次提交重试", ex);
                }
            });
        }
    }

    /** 停止序列里的最后提交: 失败只记日志, 不能挡住 consumer 关闭。 */
    private void commitProgressQuietly() {
        try {
            commitProgress(true);
        } catch (Exception e) {
            log.debug("停止时位点提交失败(忽略)", e);
        }
    }

    /** 等待 worker 队列排干并退出(running=false 后 worker 处理完剩余任务即自行结束) */
    private void drainWorkers() {
        Worker[] pool = workers;
        if (pool == null) {
            return;
        }
        for (Worker worker : pool) {
            worker.joinQuietly();
        }
    }

    private Map<String, Object> config() {
        BrokerProperties.KafkaProperties kafka = properties.kafka();
        String brokerId = properties.id();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers());
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        // 每节点独立 group.id —— 这是广播语义的来源(下行为 "-downlink" 后缀)
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG, kafka.resolvedGroupId(brokerId) + groupIdSuffix());
        cfg.put(ConsumerConfig.CLIENT_ID_CONFIG,
                KafkaMessageProducer.prefix(kafka) + "-" + brokerId + clientIdSuffix());
        // 手动提交, 保证处理后才推进 offset
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset());
        cfg.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        cfg.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15_000);
        return cfg;
    }

    /**
     * 单个消费 worker: 从共享队列争抢任务。
     * 队列满时 poller 阻塞(背压上游), 不丢弃消息。
     */
    private final class Worker {

        private final Thread thread;

        Worker(String name) {
            this.thread = new Thread(this::loop, name);
            this.thread.setDaemon(true);
        }

        void start() {
            thread.start();
        }

        private void loop() {
            while (true) {
                try {
                    Runnable task = workQueue.poll(200, TimeUnit.MILLISECONDS);
                    if (task == null) {
                        // 已停止且队列排干 → 退出; 仍在运行则继续等下一批
                        if (!running && workQueue.isEmpty()) {
                            return;
                        }
                        continue;
                    }
                    task.run();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        void joinQuietly() {
            try {
                thread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
