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
package online.ipuff.jmqtt.cluster;

/**
 * 集群内部转发消息。
 *
 * <p>同时服务于两个场景: 跨节点广播(消息面)与上行出口转发(数据面)。
 *
 * <p>用不可变记录表达 —— 它会跨线程、跨序列化边界传递, 可变对象在这里迟早出问题。
 *
 * @param brokerId 来源 broker 标识, 用于集群内去重(不回发自身)
 * @param clientId 发布方 clientId
 * @param topic    主题
 * @param qos      发布 QoS
 * @param payload  消息体
 * @param retain   是否保留消息
 * @param dup      是否重发
 * @param username 发布方的认证用户名, 可能为 null —— 仅数据面上行信封使用,
 *                 消息面(集群广播)不序列化它
 */
public record InternalMessage(
        String brokerId,
        String clientId,
        String topic,
        int qos,
        byte[] payload,
        boolean retain,
        boolean dup,
        String username
) {
}
