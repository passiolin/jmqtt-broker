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
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * 单机模式的集群总线: 不做任何跨节点投递。
 *
 * <p>由 {@code jmqtt.broker.kafka.enabled=false}(默认)激活。
 * 后续接入 Kafka 总线时, 其实现以 {@code kafka.enabled=true} 作为激活条件,
 * 两者互斥, 调用方无感知。
 *
 * <p>这样设计的原因: 集群能力依赖消息总线, 没有总线就没有跨节点投递。
 * 用同一个开关驱动装配, 避免出现「声称开了集群但实际没有总线」的静默失效。
 */
@Component
@ConditionalOnProperty(prefix = "jmqtt.broker.kafka", name = "enabled",
        havingValue = "false", matchIfMissing = true)
public class LocalClusterBus implements ClusterBus {

    private static final Logger log = LoggerFactory.getLogger(LocalClusterBus.class);

    public LocalClusterBus(BrokerProperties properties) {
        if (properties.clusterEnabled()) {
            log.warn("cluster-enabled=true 但 jmqtt.broker.kafka.enabled=false, "
                    + "已退化为单机模式: 消息只在本节点投递, 不会跨节点转发");
        } else {
            log.info("集群功能未启用, 使用单机消息总线(仅本节点投递)");
        }
    }

    @Override
    public boolean enabled() {
        return false;
    }

    @Override
    public void publish(InternalMessage message) {
        // 单机模式: 消息仅在本节点投递, 无出站流量
        log.trace("单机模式, 跳过总线投递: topic={}", message.topic());
    }
}
