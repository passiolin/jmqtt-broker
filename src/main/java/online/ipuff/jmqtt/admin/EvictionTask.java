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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 一次驱逐任务的执行状态。
 *
 * <p>规范化后的参数在构造时固定下来(不再受后续配置变更影响), 进度用原子计数器累加 ——
 * 执行线程写、Redis 上报线程与状态查询读。
 *
 * <h2>为什么单独记 {@code nodeConnections}</h2>
 * {@code evicted} 是「<b>请求断开</b>了多少条」, 而 {@code nodeConnections} 是
 * 「本节点此刻<b>真的</b>还有多少条连接」。两者不是互补关系:
 * <ul>
 *   <li>连接关闭是异步的 —— {@code channel.close()} 返回时 TCP 还没断干净,
 *       断开计数已经 +1, 实际连接数稍后才降下来;</li>
 *   <li>被驱逐的客户端可能在几秒内重连回来 —— 如果负载均衡没有按预期把新连接
 *       引走, 它们会回到同一个节点。此时断开计数一直在涨, 而连接数不降反升。</li>
 * </ul>
 * 只看「已断开多少」会让操作者以为一切顺利。真实的判据是<b>本节点连接数在下降</b>,
 * 以及<b>其他节点连接数在上升</b> —— 所以两个数都要报出来。
 */
public final class EvictionTask {

    /** 上报给 Redis 的样本客户端数上限 */
    private static final int SAMPLE_LIMIT = 20;

    private final String taskId;
    private final String nodeId;

    // ---- 规范化后的执行参数(构造后不再变化) ----
    private final String mode;
    private final double requestedValue;
    private final int requestedTarget;
    private final int target;
    private final boolean cappedByLimit;
    private final int batchSize;
    private final int intervalMs;
    private final boolean publishWill;
    private final String clientIdPrefix;
    private final int candidateCount;
    private final long issuedAt;
    private final long startedAt = System.currentTimeMillis();

    // ---- 进度 ----
    private final AtomicInteger evicted = new AtomicInteger();
    private final AtomicInteger failed = new AtomicInteger();
    private final AtomicInteger remaining = new AtomicInteger();
    private final AtomicBoolean abortRequested = new AtomicBoolean();
    private final Deque<String> sample = new ArrayDeque<>();

    private volatile EvictionState state = EvictionState.RECEIVED;
    private volatile String message;
    private volatile long finishedAt;
    private volatile int nodeConnections;

    public EvictionTask(String taskId, String nodeId, EvictionSpec spec,
                        int candidateCount, int requestedTarget, int target,
                        int batchSize, int intervalMs, long issuedAt) {
        this.taskId = taskId;
        this.nodeId = nodeId;
        this.mode = spec.byRatio() ? EvictionSpec.MODE_RATIO : EvictionSpec.MODE_COUNT;
        this.requestedValue = spec.value();
        this.clientIdPrefix = spec.clientIdPrefix();
        this.publishWill = spec.publishWillOrTrue();
        this.candidateCount = candidateCount;
        this.requestedTarget = requestedTarget;
        this.target = target;
        this.cappedByLimit = requestedTarget > target;
        this.batchSize = batchSize;
        this.intervalMs = intervalMs;
        this.remaining.set(target);
        this.issuedAt = issuedAt;
    }

    // ------------------------------------------------------------------
    // 状态跃迁(只由执行线程与中止调用方触发)
    // ------------------------------------------------------------------

    public void reject(String reason) {
        this.state = EvictionState.REJECTED;
        this.message = reason;
        this.finishedAt = System.currentTimeMillis();
    }

    public void fail(String reason) {
        this.state = EvictionState.FAILED;
        this.message = reason;
        this.finishedAt = System.currentTimeMillis();
    }

    public void finish(EvictionState finalState, String detail) {
        this.state = finalState;
        this.message = detail;
        this.finishedAt = System.currentTimeMillis();
    }

    public void markRunning() {
        this.state = EvictionState.RUNNING;
    }

    public void requestAbort() {
        this.abortRequested.set(true);
    }

    public void recordEvicted(String clientId) {
        evicted.incrementAndGet();
        synchronized (sample) {
            if (sample.size() < SAMPLE_LIMIT) {
                sample.addLast(clientId);
            }
        }
    }

    public void recordFailed() {
        failed.incrementAndGet();
    }

    // ------------------------------------------------------------------
    // Redis 上报
    // ------------------------------------------------------------------

    /**
     * 拼成 Redis hash 的字段。全部用字符串, 避免控制台侧再做一次类型推断。
     */
    public Map<String, String> toRedisFields() {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("taskId", taskId);
        fields.put("node", nodeId);
        fields.put("state", state.name());
        fields.put("mode", mode);
        fields.put("requestedValue", String.valueOf(requestedValue));
        fields.put("requestedTarget", String.valueOf(requestedTarget));
        fields.put("target", String.valueOf(target));
        fields.put("capped", String.valueOf(cappedByLimit));
        fields.put("candidateCount", String.valueOf(candidateCount));
        fields.put("batchSize", String.valueOf(batchSize));
        fields.put("intervalMs", String.valueOf(intervalMs));
        fields.put("publishWill", String.valueOf(publishWill));
        fields.put("evicted", String.valueOf(evicted.get()));
        fields.put("failed", String.valueOf(failed.get()));
        fields.put("remaining", String.valueOf(remaining.get()));
        fields.put("nodeConnections", String.valueOf(nodeConnections));
        fields.put("issuedAt", String.valueOf(issuedAt));
        fields.put("startedAt", String.valueOf(startedAt));
        fields.put("updatedAt", String.valueOf(System.currentTimeMillis()));
        if (finishedAt > 0) {
            fields.put("finishedAt", String.valueOf(finishedAt));
        }
        if (clientIdPrefix != null && !clientIdPrefix.isEmpty()) {
            fields.put("clientIdPrefix", clientIdPrefix);
        }
        if (message != null) {
            fields.put("message", message);
        }
        synchronized (sample) {
            if (!sample.isEmpty()) {
                fields.put("sample", String.join(",", sample));
            }
        }
        return fields;
    }

    // ---- 读取(上报线程与状态查询用) ----

    public String taskId() {
        return taskId;
    }

    public String nodeId() {
        return nodeId;
    }

    public EvictionState state() {
        return state;
    }

    public boolean isTerminal() {
        return state.terminal();
    }

    public boolean isAbortRequested() {
        return abortRequested.get();
    }

    public int target() {
        return target;
    }

    public int batchSize() {
        return batchSize;
    }

    public int intervalMs() {
        return intervalMs;
    }

    public boolean publishWill() {
        return publishWill;
    }

    public String clientIdPrefix() {
        return clientIdPrefix;
    }

    public boolean cappedByLimit() {
        return cappedByLimit;
    }

    public void setRemaining(int value) {
        remaining.set(Math.max(0, value));
    }

    public void setNodeConnections(int value) {
        this.nodeConnections = value;
    }
}
