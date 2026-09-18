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
package online.ipuff.jmqtt.message;

/**
 * 遗嘱消息。
 *
 * <p>只保留领域字段, <b>不把编解码层的报文对象塞进来</b>:
 * 会话需要跨进程、跨版本持久化, 一旦携带 Netty 的报文对象, 序列化就会丢失协议版本相关属性,
 * 而且把编解码细节耦合进了会话模型。报文由投递侧按需构建。
 *
 * @param topic   遗嘱主题
 * @param payload 遗嘱内容
 * @param qos     遗嘱 QoS
 * @param retain  是否为保留消息
 * @param willDelaySeconds v5 的 Will Delay Interval(秒)。
 *        <b>当前只解析保存, 不做延迟投递</b>: 延迟投递需要一个独立的定时器,
 *        并且要会与「会话过期回收」的职责划清边界(会话到期与遗嘱延迟到期是两件事)。
 *        把它丢在解码层不存, 会让 v5 客户端的这个语义无声消失 —— 所以先存下来。
 */
public record WillMessage(String topic, byte[] payload, int qos, boolean retain,
                          long willDelaySeconds) {

    /** v3.1.1 没有遗嘱延迟, 默认 0 */
    public WillMessage(String topic, byte[] payload, int qos, boolean retain) {
        this(topic, payload, qos, retain, 0L);
    }
}
