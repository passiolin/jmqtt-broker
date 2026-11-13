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
package online.ipuff.jmqtt.session;

import javax.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * 会话持久层(Redis)的阻塞 I/O 执行器。
 *
 * <h2>为什么必须有这一层</h2>
 * CONNECT / DISCONNECT 处理跑在 Netty event loop 上, 而 Redis 客户端命令是同步的:
 * 一次持久会话接入要串行做 restore(2 次 HGETALL) + acquireOwner(EVAL) +
 * saveSession(EVAL) + inflight 镜像(HGETALL)。这些 RTT 原本直接占着 event loop,
 * 连接风暴时 16 个 event loop 全卡在等 Redis, MQTT 读写一起停。
 *
 * <h2>为什么按 clientId 分片</h2>
 * 同一 clientId 的「取归属 → 写会话 → 断开清理」必须保持顺序, 否则迟到的
 * removeIfOwner 会把刚写好的会话删掉。按 clientId 哈希到固定线程:
 * 同客户端串行, 不同客户端并行。
 *
 * <h2>拒绝策略</h2>
 * 队列满时用 {@link ThreadPoolExecutor.CallerRunsPolicy} —— 让提交方(event loop)
 * 自己跑这次 Redis 操作, 相当于把压力顶回上游, 而不是静默丢掉会话操作。
 * 这只在队列堆到 2 万时发生, 意味着 Redis 已经严重不可用, 此时任何策略都只能止损。
 */
@Component
public class SessionIoExecutor {

    private static final Logger log = LoggerFactory.getLogger(SessionIoExecutor.class);

    private static final int SHARDS = Math.max(8, Runtime.getRuntime().availableProcessors() * 2);

    private static final int QUEUE_PER_SHARD = 20_000;

    private final ThreadPoolExecutor[] shards;
    private final AtomicInteger taskCount = new AtomicInteger();

    public SessionIoExecutor() {
        this.shards = new ThreadPoolExecutor[SHARDS];
        for (int i = 0; i < SHARDS; i++) {
            ThreadFactory factory = new NamedFactory("jmqtt-session-io-" + i);
            ThreadPoolExecutor shard = new ThreadPoolExecutor(
                    1, 1, 0L, TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(QUEUE_PER_SHARD), factory,
                    new ThreadPoolExecutor.CallerRunsPolicy());
            shard.allowCoreThreadTimeOut(false);
            shards[i] = shard;
        }
    }

    /** 提交一个带返回值的会话 I/O 任务; 同 clientId 固定同一分片。 */
    public <T> CompletableFuture<T> submit(String clientId, Supplier<T> task) {
        try {
            return CompletableFuture.supplyAsync(() -> {
                taskCount.incrementAndGet();
                return task.get();
            }, shardFor(clientId));
        } catch (RejectedExecutionException e) {
            // 执行器已关闭: 退化为调用方同步执行, 不能把 CONNECT/DISCONNECT 操作吞掉
            taskCount.incrementAndGet();
            return CompletableFuture.completedFuture(task.get());
        }
    }

    /** 提交一个无返回值的会话 I/O 任务。 */
    public CompletableFuture<Void> execute(String clientId, Runnable task) {
        try {
            return CompletableFuture.runAsync(() -> {
                taskCount.incrementAndGet();
                task.run();
            }, shardFor(clientId));
        } catch (RejectedExecutionException e) {
            taskCount.incrementAndGet();
            task.run();
            return CompletableFuture.completedFuture(null);
        }
    }

    /** 已执行的会话 I/O 任务数(观测用)。 */
    public int executedTasks() {
        return taskCount.get();
    }

    private ThreadPoolExecutor shardFor(String clientId) {
        int hash = clientId == null ? 0 : clientId.hashCode();
        return shards[Math.floorMod(hash, shards.length)];
    }

    @PreDestroy
    void shutdown() {
        for (ThreadPoolExecutor shard : shards) {
            shard.shutdown();
        }
        log.info("会话 I/O 执行器已停止, 累计任务={}", taskCount.get());
    }

    private static final class NamedFactory implements ThreadFactory {

        private final String prefix;

        NamedFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, prefix);
            thread.setDaemon(true);
            return thread;
        }
    }
}
