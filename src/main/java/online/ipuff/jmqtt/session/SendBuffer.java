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
package online.ipuff.jmqtt.session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

/**
 * 单个连接的发送缓冲: 在途窗口 + 有界排队。
 *
 * <h2>它解决的是哪个问题</h2>
 * 对一个连上就不再读取 socket 的客户端, Netty 的写缓冲会无限增长 ——
 * 一个坏客户端就足以把整个 broker 的堆吃光。这是可用性级别的缺陷, 不是性能问题。
 *
 * <h2>两道闸门, 职责不同</h2>
 * <ol>
 *   <li><b>在途窗口({@code maxInflight})</b> —— 语义闸门, 就是 MQTT 的流控:
 *       未确认的 QoS 1/2 消息数达到上限后不再继续发, 剩下的排队等确认。
 *       这是规范行为, 也是<em>慢消费者的主要保护</em>: 不确认就不再发新的。</li>
 *   <li><b>队列上限({@code maxMqueueLen})</b> —— 内存闸门: 排队长度到顶后丢弃最旧的,
 *       保证单个客户端占用的内存有硬上限。</li>
 * </ol>
 * 为什么主要靠窗口而不是靠队列: 队列是「已经决定要发」的消息, 只能兜住瞬时抖动;
 * 而窗口会让整个投递节奏跟随客户端的确认速度 —— 慢客户端自然就慢下来,
 * 不会堆积。单靠队列的话, 内存上限一到就必然开始丢消息。
 *
 * <h2>为什么按连接而不是按会话</h2>
 * 在途与排队都是「这条 TCP 连接上还没写完的东西」, 连接一断就全部失效。
 * 因此它挂在 Channel 上(见 {@code ChannelAttributes.SEND_BUFFER}), 随连接自动回收,
 * 不需要任何生命周期管理。
 *
 * <p>注意与<b>离线队列</b>的区别: 离线队列属于会话, 要跨连接、跨节点存活;
 * 发送缓冲属于连接, 只在连接存活期内有效。两者共用「有界 + 丢最旧 + 计数」的口径,
 * 但生命周期完全不同, 不能混为一谈。
 */
public final class SendBuffer {

    private static final Logger log = LoggerFactory.getLogger(SendBuffer.class);

    /** 一条待写出的消息 */
    public record Outbound(String topic, int qos, byte[] payload, boolean retain, boolean dup) {
    }

    private final int maxInflight;
    private final int maxQueueLen;
    private final BackpressureMetrics metrics;

    private final Deque<Outbound> queue = new ArrayDeque<>();

    /** 已发出、等待确认的报文标识符 */
    private final Set<Integer> inflightPacketIds = new HashSet<>();

    /**
     * 已占位但尚未确定报文标识符的窗口数。
     *
     * <p>需要它是因为「申请窗口」与「分配标识符」不是一步: 多个线程可能同时通过
     * 窗口检查, 各自占位后再分配标识符。没有这个计数器就会短暂超出窗口上限。
     */
    private int reserved;

    public SendBuffer(int maxInflight, int maxQueueLen, BackpressureMetrics metrics) {
        this.maxInflight = Math.max(1, maxInflight);
        this.maxQueueLen = Math.max(0, maxQueueLen);
        this.metrics = metrics;
    }

    // ------------------------------------------------------------------
    // 窗口
    // ------------------------------------------------------------------

    /**
     * 申请一个发送窗口。返回 true 表示已占位, 调用方必须随后调用
     * {@link #confirm(int)} 或 {@link #cancelReservation()}。
     */
    public synchronized boolean tryAcquireWindow() {
        if (inflightPacketIds.size() + reserved >= maxInflight) {
            return false;
        }
        reserved++;
        return true;
    }

    /**
     * 发送完成后把占位换成真实的报文标识符。
     */
    public synchronized void confirm(int packetId) {
        if (reserved > 0) {
            reserved--;
        }
        inflightPacketIds.add(packetId);
    }

    /**
     * 放弃占位(发送前发现连接已不可用等)。
     */
    public synchronized void cancelReservation() {
        if (reserved > 0) {
            reserved--;
        }
    }

    /**
     * 释放一个窗口(收到 PUBACK / PUBREC)。
     *
     * @return 是否确实释放了(重复确认或未知标识符返回 false)
     */
    public synchronized boolean release(int packetId) {
        return inflightPacketIds.remove(packetId);
    }

    public synchronized int inflightCount() {
        return inflightPacketIds.size() + reserved;
    }

    public synchronized boolean hasWindow() {
        return inflightPacketIds.size() + reserved < maxInflight;
    }

    // ------------------------------------------------------------------
    // 队列
    // ------------------------------------------------------------------

    /**
     * 入队。超限时丢弃最旧的。
     *
     * @return 本次丢弃条数(0 表示没有丢弃)
     */
    public synchronized int enqueue(Outbound message) {
        if (maxQueueLen <= 0) {
            metrics.queueFullDropped();
            log.debug("发送队列上限为 0, 丢弃消息: topic={}", message.topic());
            return 1;
        }
        queue.addLast(message);
        int dropped = 0;
        // 丢最旧: 与离线队列同一口径。慢消费者场景下, 让它尽快追到「当前」
        // 比让它按顺序补完历史更有意义 —— 补历史只会让它更慢
        while (queue.size() > maxQueueLen) {
            queue.pollFirst();
            dropped++;
        }
        if (dropped > 0) {
            metrics.queueFullDropped(dropped);
            log.warn("发送队列超限, 丢弃最旧的 {} 条: 上限 {} 当前 {}。"
                            + "客户端消费速度跟不上投递速度, 或 write buffer 长期不可写",
                    dropped, maxQueueLen, queue.size());
        }
        return dropped;
    }

    /**
     * 在窗口允许的前提下取出一条。
     *
     * @return 待发送消息; 没有窗口或队列为空时返回 {@code null}
     */
    public synchronized Outbound pollIfWindow() {
        if (!hasWindow() || queue.isEmpty()) {
            return null;
        }
        Outbound next = queue.pollFirst();
        if (next != null) {
            reserved++;
        }
        return next;
    }

    public synchronized int queuedCount() {
        return queue.size();
    }

    /**
     * 丢弃队列中全部 QoS 0 消息(连接即将关闭时无需再写)。
     */
    public synchronized int clear() {
        int size = queue.size();
        queue.clear();
        return size;
    }
}
