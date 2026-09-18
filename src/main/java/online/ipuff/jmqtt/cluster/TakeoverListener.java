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
 * 跨节点连接接管的接收侧。
 *
 * <p>MQTT 规范 [MQTT-3.1.4-2]: 当某个 clientId 已有连接时, 服务端必须断开既有连接。
 * 单节点内这件事由 {@code ConnectionRegistry.register} 顺手完成; 跨节点就需要一条
 * 节点间消息 —— 本接口就是那个消息的落点。
 *
 * <p>为什么这件事必须做: 不通知的话, 旧节点上的连接会一直存活到它自己的心跳超时
 * (可能几分钟)。这期间同一个 clientId 在两个节点上同时在线, 表现为
 * 共享订阅重复投递、下行指令被处理两次等难以复现的问题。
 */
public interface TakeoverListener {

    /**
     * 本节点持有的某个 clientId 已被其他节点接管, 应当释放。
     *
     * <p>实现必须做到三件事:
     * <ol>
     *   <li>关闭本节点的连接</li>
     *   <li>清理本节点的运行时状态(会话、订阅、在途消息)</li>
     *   <li><b>绝不删除持久层的会话记录</b> —— 该会话此刻已归属新节点,
     *       删掉等于把新节点刚建立的会话摧毁</li>
     * </ol>
     * 另外关闭顺序有讲究: <b>必须先移除会话再关连接</b>,
     * 否则连接关闭触发的 {@code channelInactive} 会读到仍然存在的会话,
     * 从而错误地发布遗嘱消息 —— 客户端只是换了节点, 并没有死。
     *
     * @param clientId   被接管的客户端
     * @param fromNodeId 接管方节点, 仅用于日志
     */
    void onTakeover(String clientId, String fromNodeId);
}
