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

/**
 * 接收方向的 QoS 2 去重状态。
 *
 * <h2>它防的是什么</h2>
 * QoS 2 的「恰好一次」是<b>双方各自保证</b>的: 发送方保证不重复发送, 接收方保证不重复投递。
 * 只做一半等于没做 —— 消息不重复发出来了, 但接收方因为没记住「这个报文标识符我已经收过」
 * 而把它投递两遍。
 *
 * <h2>为什么会重复到达</h2>
 * 客户端发出 QoS 2 PUBLISH 后等 PUBREC。若 PUBREC 在途中丢失(或客户端超时太短),
 * 客户端会带着 {@code dup=1} <b>重发同一个报文标识符</b>。这是<b>合法行为</b>,
 * 规范的解法就是接收方按标识符去重。
 *
 * <p>不实现去重的后果很具体: 同一台设备收到两条相同的控制指令(开关被切了两次、
 * 计量被重复记账), 而发布方从协议层的角度看「一切正常」—— 它只是重试了一次，
 * 并且拿到了两次成功的确认。
 *
 * <h2>状态何时释放</h2>
 * 收到 PUBREL 时释放。这一条同样重要: 只记不释放会让同一个报文标识符永远无法再用于
 * 新一轮流程, 于是后续消息被<b>静默丢弃</b> —— 那比重复投递更糟, 因为重复看得见、丢弃看不见。
 *
 * <h2>当前边界</h2>
 * 状态在进程内, <b>不跨节点重启</b>。规范把「已接收但未完成确认的 QoS 2 消息」列为会话状态的一部分，
 * 因此严格说它应当随会话持久化。这里没有做的原因是: 要覆盖的场景是「客户端在秒级内重发」，
 * 而重启窗口内恰好出现「会话被恢复 + 客户端重发同一标识符」的概率极低。
 * 这一点在 {@code docs/reports/10-qos2-inbound-dedup.md} 里明确记为遗留。
 */
public interface IInboundQos2Store {

    /**
     * 标记一个报文标识符的处理结果。
     */
    enum Result {
        /** 首次收到: 应当投递 */
        FIRST,
        /** 重复: 必须丢弃, 只补一个 PUBREC */
        DUPLICATE,
        /** 该客户端的未完成标识符数超出上限: 调用方应断开连接 */
        OVERFLOW
    }

    /**
     * 标记「已收到该标识符的 QoS 2 PUBLISH, 尚未收到 PUBREL」。
     *
     * @return 见 {@link Result}
     */
    Result mark(String clientId, int packetId);

    /**
     * PUBREL 到达, 释放该标识符, 之后可以再次用于新一轮流程。
     */
    void release(String clientId, int packetId);

    /**
     * 清除该客户端的全部在途标识符。会话销毁 / 接管时调用。
     */
    void clearClient(String clientId);

    /**
     * 当前被跟踪的客户端数, 用于观测是否存在泄漏(应当随会话销毁而下降)。
     */
    int clientCount();
}
