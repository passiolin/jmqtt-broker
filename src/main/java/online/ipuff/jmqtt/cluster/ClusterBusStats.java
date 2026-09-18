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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 集群总线的运行时指标。
 *
 * <p>单独成一个接口, 避免把指标方法塞进 {@link ClusterBus} 而污染其语义。
 * 实现方按需实现, 消费方通过 {@code ObjectProvider} 可选注入。
 *
 * <p>其中 {@code dropped} 与 {@code outboxSize} 是最需要盯的两个:
 * 前者上升说明写总线跟不上发布速率(或 Kafka 不可用), 后者持续接近容量上限
 * 说明即将开始丢弃。这两个指标组合起来, 能把「跨节点投递正在悄悄变差」
 * 这件事在丢消息之前暴露出来。
 */
public interface ClusterBusStats {

    /**
     * 指标键值对。
     */
    Map<String, Long> stats();

    /**
     * 通用实现: 空指标。
     */
    static Map<String, Long> empty() {
        return new LinkedHashMap<>();
    }
}
