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

import online.ipuff.jmqtt.config.BrokerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 出站转发: 把本节点产生的消息交给集群总线。
 *
 * <p><b>出站的两件事应当拆开</b>, 因为流量特征完全不同:
 * <ul>
 *   <li><b>数据面</b>: 上行遥测数据量大, 只需写一次给下游消费, 不需要每个节点都读</li>
 *   <li><b>消息面</b>: 跨节点 pub/sub 需要每个节点都读到(差异化 group.id 广播)</li>
 * </ul>
 * 混在一条链路上会让数据面白白承受 N 倍扇出。
 *
 * <p><b>数据面是服务端接入的正规路径</b>: 平台侧消费遥测不该通过 MQTT 订阅
 * （包括共享订阅）, 而应直连 Kafka —— 理由见 {@code docs/server-side-ingestion.md}。
 * 因此这里的分流不是可选的优化, 而是把两类流量分开的必要一步:
 * 命中的消息写进数据面 topic 后即视为「已交付下游」, 不再进集群广播。
 */
@Service
public class InternalCommunication {

    private static final Logger log = LoggerFactory.getLogger(InternalCommunication.class);

    private final ClusterBus clusterBus;

    private final BrokerProperties properties;

    public InternalCommunication(ClusterBus clusterBus, BrokerProperties properties) {
        this.clusterBus = clusterBus;
        this.properties = properties;
    }

    /**
     * 出站转发: 把本节点产生的消息交给集群总线。
     *
     * <p>按两条链路分流:
     * <ol>
     *   <li><b>数据面</b>: 命中上行过滤器的消息写入 {@code uplink-topic},
     *       供下游(数仓 / 业务系统)消费一次即可, 不需要每个 broker 都读到。</li>
     *   <li><b>消息面</b>: 写入 {@code cluster-topic}, 其他节点以各自独立的
     *       group.id 消费, 实现跨节点 pub/sub。</li>
     * </ol>
     *
     * <p><b>{@code uplink-exclusive=true} 时, 命中上行的消息不再进消息面。</b>
     * 这是把「上行不走集群广播」这条架构判断落成开关: 当上行订阅者集中在云端
     * (而非分布在各 broker)时, N 倍扇出纯粹是浪费, 关掉它可把扇出从 N 倍降到 1 倍。
     */
    public void internalSend(InternalMessage message) {
        if (message.topic() == null || message.topic().isEmpty()) {
            log.warn("跳过空主题消息的出站, clientId={}", message.clientId());
            return;
        }

        boolean uplink = clusterBus.uplinkEnabled() && clusterBus.isUplink(message.topic());
        if (uplink) {
            clusterBus.publishUplink(message);
        }

        if (uplink && properties.kafka().uplinkExclusive()) {
            // 已走数据面, 且配置为独占 —— 不再进集群广播
            log.trace("上行消息走数据面(独占), 跳过集群广播: topic={}", message.topic());
            return;
        }

        if (!clusterBus.enabled()) {
            return;
        }
        // 广播开关: 业务上不存在跨节点 MQTT 订阅时, 「每节点消费全量消息」只是成本。
        // 这里只挡消息面 —— 数据面上行(上面已发)与连接接管(独立方法)都不受影响,
        // 后者必须照常工作, 否则会话归属转移会失效。见 docs/cluster-broadcast-rationale.md
        if (!clusterBus.isBroadcastAllowed(message.topic())) {
            log.trace("主题未命中广播策略, 跳过消息面: topic={}", message.topic());
            return;
        }
        clusterBus.publish(message);
    }

    /**
     * 构造一条带来源标识的内部消息。
     *
     * <p>来源标识是回环防护的依据: 消费端注入总线消息时必须标记来源,
     * 出站侧据此跳过, 否则「每节点消费全量」的拓扑会指数级重发。
     */
    public InternalMessage fromLocal(String clientId, String topic, int qos,
                                     byte[] payload, boolean retain, boolean dup, String username) {
        return new InternalMessage(properties.id(), clientId, topic, qos, payload, retain, dup, username);
    }
}
