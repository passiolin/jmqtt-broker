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

import io.netty.channel.Channel;
import javax.annotation.PreDestroy;
import online.ipuff.jmqtt.config.BrokerProperties;
import online.ipuff.jmqtt.handler.ChannelAttributes;
import online.ipuff.jmqtt.session.ConnectionRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * 驱逐执行器: 分批断开本节点上的客户端连接。
 *
 * <h2>驱逐做什么、不做什么</h2>
 * <b>只关连接。会话、订阅、离线队列、在途镜像一律不动。</b>
 * 这是整个操作能成立的前提: 被驱逐的客户端重连到另一个节点后, 会走既有的
 * 「取得归属 + 跨节点接管」路径 —— 它拿到的 {@code sessionPresent=true},
 * 订阅被恢复、离线积压继续投递、未确认的 QoS 1/2 以 {@code dup=1} 重发。
 * 反过来, 如果顺手把会话删了, 客户端重连后会以为自己是全新的, 表现为
 * 「订阅没了、离线消息没了」, 而这恰恰是驱逐本不该造成的破坏。
 *
 * <h2>为什么要分批、限速, 而不是一次断完</h2>
 * 一次性断开十万条连接会同时触发三件事: 十万条遗嘱涌入路由与集群总线、十万次重连
 * 打向剩余节点、以及十万条会话的归属转移。任一件都足以把集群从「一次运维」变成
 * 「一次故障」。所以这里强制分批, 并且有数量上限与间隔下限 ——
 * <b>这两个约束是安全阀, 不是性能调优参数</b>, 因此做成配置而不是请求参数
 * (请求只能在配置允许的范围内调整)。
 *
 * <h2>为什么严格串行</h2>
 * 单线程执行, 同一时刻只允许一个任务。两个任务并行时会各自按自己的节奏断开,
 * 合起来的速率就不再是任何一方设定的值 —— 限速这件事只有在「谁在限速」唯一时才有意义。
 *
 * @see AdminRedisKeys 命令与结果的键约定
 */
@Component
public class EvictionService {

    private static final Logger log = LoggerFactory.getLogger(EvictionService.class);

    private final AdminProperties properties;
    private final ConnectionRegistry connectionRegistry;
    private final AdminCommandReporter reporter;

    /** 本节点标识, 随任务一起上报, 供控制台定位是哪个节点在断 */
    private final String nodeId;

    /** 单线程: 见类注释「为什么严格串行」 */
    private final ExecutorService executor;

    private final Object lock = new Object();

    private volatile EvictionTask current;

    public EvictionService(AdminProperties properties,
                           BrokerProperties brokerProperties,
                           ConnectionRegistry connectionRegistry,
                           AdminCommandReporter reporter) {
        this.properties = properties;
        this.nodeId = brokerProperties.id();
        this.connectionRegistry = connectionRegistry;
        this.reporter = reporter;
        this.executor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "jmqtt-eviction");
            thread.setDaemon(true);
            return thread;
        });
    }

    // ------------------------------------------------------------------
    // 提交 / 中止
    // ------------------------------------------------------------------

    /**
     * 提交一次驱逐任务。
     *
     * <p><b>不抛异常</b>: 任何拒绝都表达为返回一个状态为 {@link EvictionState#REJECTED}
     * 的任务对象, 并已写入 Redis。调用方(命令轮询线程)没有恢复动作可做,
     * 让它去处理异常只会多一条无意义的错误日志 —— 而「拒绝原因」对操作者是有用的信息,
     * 必须落到他能看到的地方。
     */
    public EvictionTask submit(String taskId, EvictionSpec spec, long issuedAt) {
        synchronized (lock) {
            EvictionTask running = current;
            if (running != null && !running.isTerminal()) {
                EvictionTask rejected = rejection(taskId, "本节点已有驱逐任务在执行: " + running.taskId());
                log.warn("驱逐任务被拒绝, 已有任务在执行: taskId={} running={}", taskId, running.taskId());
                return rejected;
            }
            try {
                spec.validate();
            } catch (IllegalArgumentException e) {
                return rejection(taskId, e.getMessage());
            }

            List<String> candidates = selectCandidates(spec.clientIdPrefix());
            if (candidates.isEmpty()) {
                return rejection(taskId, "没有匹配的在线客户端" + prefixHint(spec.clientIdPrefix()));
            }

            int requestedTarget = spec.byRatio()
                    ? (int) Math.ceil(candidates.size() * spec.value())
                    : (int) Math.floor(spec.value());
            requestedTarget = Math.max(1, Math.min(requestedTarget, candidates.size()));

            int target = Math.min(requestedTarget, properties.maxEvictPerTask());
            int batchSize = normalizeBatchSize(spec.batchSize());
            int intervalMs = normalizeIntervalMs(spec.intervalMs());

            EvictionTask task = new EvictionTask(taskId, nodeId, spec, candidates.size(),
                    requestedTarget, target, batchSize, intervalMs, issuedAt);
            if (task.cappedByLimit()) {
                log.warn("驱逐请求 {} 条被上限裁剪为 {} 条(配置 jmqtt.broker.admin.max-evict-per-task)。"
                                + "分批慢速驱逐是为了避免遗嘱风暴与重连风暴, 请分多次执行。",
                        requestedTarget, target);
            }
            current = task;
            task.markRunning();
            report(task);
            executor.execute(() -> run(task, candidates));
            return task;
        }
    }

    /**
     * 中止当前任务。已断开的部分不回滚 —— 也不需要: 会话与订阅都还在, 客户端会自己回来。
     */
    public boolean abort(String taskId) {
        EvictionTask task = current;
        if (task == null || task.isTerminal()) {
            return false;
        }
        if (taskId != null && !taskId.isEmpty() && !task.taskId().equals(taskId)) {
            return false;
        }
        task.requestAbort();
        log.info("收到中止请求: taskId={}", task.taskId());
        return true;
    }

    public EvictionTask current() {
        return current;
    }

    // ------------------------------------------------------------------
    // 执行
    // ------------------------------------------------------------------

    private void run(EvictionTask task, List<String> candidates) {
        // 打乱候选顺序。不这样做的话「按比例驱逐」每次都会命中同一批人 ——
        // 对设备侧而言, 少数设备被反复踢下线、其余从不被动, 是很糟的分布。
        Collections.shuffle(candidates, ThreadLocalRandom.current());

        int target = task.target();
        int index = 0;
        long lastLogAt = System.currentTimeMillis();
        log.info("开始驱逐: taskId={} 候选={} 计划={} 每批={} 间隔={}ms 发布遗嘱={}",
                task.taskId(), candidates.size(), target, task.batchSize(), task.intervalMs(),
                task.publishWill());

        try {
            while (index < target) {
                if (task.isAbortRequested()) {
                    task.finish(EvictionState.ABORTED,
                            "已中止: 计划 " + target + " 条, 实际断开 " + index + " 条");
                    log.info("驱逐已中止: taskId={} 已断开={}", task.taskId(), index);
                    return;
                }

                int batch = Math.min(task.batchSize(), target - index);
                int closed = 0;
                for (int i = 0; i < batch; i++) {
                    String clientId = candidates.get(index++);
                    if (disconnect(clientId, task.publishWill())) {
                        closed++;
                        task.recordEvicted(clientId);
                    } else {
                        // 候选集是先快照再执行的, 期间客户端可能已自行断开 —— 这在长任务里很正常
                        task.recordFailed();
                    }
                }
                task.setRemaining(target - index);
                task.setNodeConnections(connectionRegistry.size());
                report(task);

                if (log.isInfoEnabled() && System.currentTimeMillis() - lastLogAt > 30_000) {
                    lastLogAt = System.currentTimeMillis();
                    log.info("驱逐进行中: taskId={} 已断开={}/{} 本节点连接数={}",
                            task.taskId(), index, target, connectionRegistry.size());
                }

                if (index < target) {
                    sleep(task.intervalMs());
                }
                if (closed == 0 && index >= target) {
                    break;
                }
            }
            task.finish(EvictionState.COMPLETED, "已完成: 计划 " + target + " 条, 实际断开 "
                    + task.toRedisFields().get("evicted") + " 条");
            log.info("驱逐完成: taskId={} 计划={} 本节点剩余连接={}",
                    task.taskId(), target, connectionRegistry.size());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            task.finish(EvictionState.ABORTED, "执行线程被中断");
        } catch (Exception e) {
            task.fail("执行出错: " + e);
            log.error("驱逐执行出错: taskId={}", task.taskId(), e);
        } finally {
            task.setNodeConnections(connectionRegistry.size());
            report(task);
        }
    }

    /**
     * 断开单个客户端。
     *
     * @param publishWill 是否按规范发布遗嘱。为 false 时置
     *                    {@link ChannelAttributes#SUPPRESS_WILL}, 由
     *                    {@code MqttBrokerHandler} 在连接关闭时跳过发布
     * @return 是否确实请求了一次断开(false = 该客户端已不在线)
     */
    public boolean disconnect(String clientId, boolean publishWill) {
        Channel channel = connectionRegistry.get(clientId);
        if (channel == null || !channel.isActive()) {
            return false;
        }
        if (!publishWill) {
            channel.attr(ChannelAttributes.SUPPRESS_WILL).set(Boolean.TRUE);
        }
        channel.close();
        return true;
    }

    /**
     * 候选集: 本节点当前在线的 clientId 快照, 按前缀筛选。
     *
     * <p>先快照再执行是刻意为之 —— 遍历连接表的同时关闭连接会改变这个表,
     * 而 ConcurrentHashMap 的弱一致性迭代在「边遍历边改」时既可能漏也可能重,
     * 于是「计划断 5000 条」就变成了一个说不准的数字。
     */
    private List<String> selectCandidates(String prefix) {
        List<String> candidates = new ArrayList<>();
        String normalized = (prefix == null || prefix.isBlank()) ? null : prefix;
        for (String clientId : connectionRegistry.clientIds()) {
            if (normalized == null || clientId.startsWith(normalized)) {
                candidates.add(clientId);
            }
        }
        return candidates;
    }

    private int normalizeBatchSize(Integer requested) {
        int value = requested == null || requested < 1 ? properties.defaultEvictBatchSize() : requested;
        return Math.max(1, value);
    }

    private int normalizeIntervalMs(Integer requested) {
        int value = requested == null || requested < 1 ? properties.minEvictIntervalMs() : requested;
        return Math.max(properties.minEvictIntervalMs(), value);
    }

    private EvictionTask rejection(String taskId, String reason) {
        EvictionTask rejected = new EvictionTask(taskId, nodeId,
                new EvictionSpec(EvictionSpec.MODE_COUNT, 1d, 1, 1, null, null),
                0, 0, 0, 1, properties.minEvictIntervalMs(), System.currentTimeMillis());
        rejected.reject(reason);
        report(rejected);
        return rejected;
    }

    private static String prefixHint(String prefix) {
        return (prefix == null || prefix.isBlank()) ? "" : "(前缀 " + prefix + ")";
    }

    private void report(EvictionTask task) {
        reporter.write(task);
    }

    private static void sleep(int millis) throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(millis);
    }

    @PreDestroy
    public void shutdown() throws InterruptedException {
        EvictionTask task = current;
        if (task != null && !task.isTerminal()) {
            task.requestAbort();
        }
        executor.shutdownNow();
        // 略等一会, 让任务把 ABORTED 状态写出去 —— 否则控制台会一直看到 RUNNING
        CountDownLatch latch = new CountDownLatch(1);
        Thread waiter = new Thread(() -> {
            try {
                executor.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            } finally {
                latch.countDown();
            }
        }, "jmqtt-eviction-shutdown");
        waiter.setDaemon(true);
        waiter.start();
        latch.await(3, TimeUnit.SECONDS);
    }
}
