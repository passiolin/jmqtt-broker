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
 * 集群消息总线。
 *
 * <p>集群广播有三种实现路线, 代价与恢复特性差别很大（见
 * {@code docs/cluster-broadcast-rationale.md}）: 复制元数据、互联点对点转发、
 * 以及经由消息总线广播。本实现选第三种 —— 每个节点消费全量消息后在本地做路由匹配。
 *
 * <h2>目标形态</h2>
 * 每个 broker 只保存本节点客户端的订阅;发布时把消息写入一条 Kafka topic;
 * 其他节点用<b>各自独立的 group.id</b> 消费同一条 topic, 于是每个节点都收到全量消息
 * —— 这正是 Kafka 官方文档描述的广播语义:
 * <blockquote>
 * If all the consumer instances have different consumer groups,
 * then each record will be broadcast to all the consumer processes.
 * </blockquote>
 *
 * <h2>三条必须遵守的约定</h2>
 * <ol>
 *   <li><b>每节点独立 group.id。</b>相同则退化为组内分摊(队列语义), 跨节点投递失效。</li>
 *   <li><b>跳过自身产生的消息。</b>发布节点在发布时已完成本地投递, 若再从总线取回
 *       自己的消息会重复投递一次。</li>
 *   <li><b>回环防护。</b>总线 ingress 注入的消息绝不能再出站。否则在
 *       「每节点消费全量」的拓扑下会呈指数级重发(1 → N → N² …)。
 *       本实现从结构上保证: ingress 只调用本节点投递, 不经过出站路径。</li>
 * </ol>
 *
 * <h2>两条链路</h2>
 * 实现可同时承载消息面(跨节点 pub/sub)与数据面(上行出口),
 * 但两者语义不同: 消息面每个节点都要读, 数据面只需下游消费一次。
 */
public interface ClusterBus {

    /**
     * 消息面是否可用(跨节点 pub/sub)。
     */
    boolean enabled();

    /**
     * 将消息投递到总线, 由其他节点消费。
     *
     * <p><b>不得阻塞调用方。</b>该方法位于 MQTT 发布路径上,
     * 实现必须自行排队, 并在队列满时选择丢弃而不是阻塞。
     *
     * @param message 内部消息, 携带来源 brokerId 以便消费端去重
     */
    void publish(InternalMessage message);

    // ------------------------------------------------------------------
    // 数据面(上行出口)
    // ------------------------------------------------------------------

    /**
     * 数据面是否可用。
     */
    default boolean uplinkEnabled() {
        return false;
    }

    /**
     * 判断该主题是否属于上行数据。
     */
    default boolean isUplink(String topic) {
        return false;
    }

    /**
     * 该主题是否允许进入集群广播。
     *
     * <p><b>这是一个决策点而不是纯查询</b>: 每个出站消息会调用它恰好一次,
     * 返回 false 会被计入 {@code broadcastSkipped} 指标 —— 那个数字是操作者唯一能
     * 判断「这个开关到底省下了多少」的依据。
     *
     * <p>默认放行。真正返回 false 的是这样一类部署: <b>业务上不存在跨节点的 MQTT 订阅</b>,
     * 于是「每节点消费全量消息」失去了存在理由。
     */
    default boolean isBroadcastAllowed(String topic) {
        return true;
    }

    /**
     * 投递到数据面, 由下游消费(数仓 / 业务系统)。
     */
    default void publishUplink(InternalMessage message) {
        // 默认不启用数据面
    }

    /**
     * 发布连接生命周期事件(上线/下线)。
     *
     * <p>事件走独立的 {@code connection-event-topic}, 供后台系统消费设备在线状态;
     * 与消息面/数据面互不相干。默认实现为空 —— 未启用 Kafka 总线的部署没有事件出口。
     *
     * <p><b>不得阻塞调用方</b>: 该方法在连接建立/断开路径上, 实现必须走有界队列。
     */
    default void publishConnectionEvent(ConnectionEvent event) {
        // 默认不启用事件出口
    }

    // ------------------------------------------------------------------
    // 控制面: 跨节点连接接管
    // ------------------------------------------------------------------

    /**
     * 通知目标节点释放某个 clientId 的旧连接。
     *
     * <p>MQTT 规范要求同一 clientId 只能有一个活动连接。单节点内这件事由
     * {@code ConnectionRegistry.register} 顺手完成; 跨节点时必须显式通知,
     * 否则旧节点上的连接会一直存活到它自己的心跳超时。
     *
     * <p>这是<b>定向</b>消息: 只通知真正持有该连接的那个节点, 不做广播。
     *
     * @param clientId     需要被释放的客户端
     * @param targetNodeId 目标节点 id
     */
    default void publishTakeover(String clientId, String targetNodeId) {
        // 单机模式无需通知: 接管在 ConnectionRegistry 内部就完成了
    }
}
