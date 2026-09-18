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
package online.ipuff.jmqtt.store;

import java.util.List;

/**
 * 离线消息队列。
 *
 * <p><b>它补的是哪个洞。</b>MQTT 规范里, 对 {@code cleanSession=0} 的客户端,
 * 服务端在它离线期间必须为 QoS 1/2 的订阅缓存消息, 并在它重连后投递。
 * 在此之前, 本实现对这个场景是<b>直接丢弃</b> —— 也就是 QoS 1/2 的投递保证
 * 在最需要它的场景(客户端掉线)下根本没有兑现。
 *
 * <h2>为什么是「每个客户端一条队」而不是丢进 Kafka</h2>
 * Kafka 擅长的是「一份数据多方消费」; 而这里是「一批消息只欠给某一个确定的客户端,
 * 且必须保序」。Kafka 的日志压缩保留的是每个 key 的最新值, 不是队列语义;
 * 用 Kafka 做每客户端队列需要自己维护逐客户端 offset, 复杂度远高于收益。
 * 因此这里用按 clientId 分键的有序队列。
 *
 * <h2>有界且有明确的丢弃口径</h2>
 * 队列必须有上限 —— 一个长期离线的客户端会无限累积。超限时<b>丢弃最旧的</b>并计数,
 * 绝不静默: 丢弃是 QoS 1/2 保证的实际破损点, 必须能在指标上看见。
 *
 * <h2>调用时机</h2>
 * 只在「投递时目标不在线」与「客户端重连」两条路径上使用。频率与离线客户端的
 * 消息量成正比 —— 那本来就是一个很小的子集, 不属于「高频写外部存储」的范畴。
 * 但仍是<b>每条消息一次写</b>, 因此不具备持久化能力时退化为纯内存。
 */
public interface IPendingMessageStore {

    /**
     * 是否具备持久化能力。为 false 时队列只在进程内, 节点重启即丢。
     */
    boolean persistent();

    /**
     * 入队。
     *
     * @return 本次因超限被丢弃的消息条数(0 表示没有丢弃)
     */
    int enqueue(String clientId, PendingMessage message, int maxLen);

    /**
     * 取出并移除最多 {@code limit} 条消息(保持入队顺序)。
     *
     * <p>分批而不是一次取空: 客户端在投递过程中断开时, 尚未取出的部分仍留在队列里,
     * 下次重连还能拿到。一次取空则这部分会随连接一起丢失。
     */
    List<PendingMessage> drainBatch(String clientId, int limit);

    /**
     * 当前积压条数。
     */
    int size(String clientId);

    /**
     * 清空该客户端的队列。
     */
    void removeAll(String clientId);

    /**
     * 累计丢弃条数, 用于监控。
     */
    long droppedCount();

    /**
     * 无持久化能力时使用的默认容量上限处理: 不限制持久层, 只做内存限制。
     */
    static int normalizedMaxLen(int maxLen) {
        return maxLen <= 0 ? 0 : maxLen;
    }
}
