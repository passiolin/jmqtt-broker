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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package online.ipuff.jmqtt;

import online.ipuff.jmqtt.metrics.NodeMetricsService;
import online.ipuff.jmqtt.session.BackpressureMetrics;
import online.ipuff.jmqtt.session.ConnectionRegistry;
import online.ipuff.jmqtt.session.QosMetrics;
import online.ipuff.jmqtt.session.SessionStoreService;
import online.ipuff.jmqtt.subscribe.SubscribeStoreService;

/**
 * 测试用的最小 NodeMetricsService 装配。
 * ObjectProvider 全部为空: 单测里不会触发 Micrometer 绑定与集群总线读取。
 */
public final class TestNodeMetrics {

    private TestNodeMetrics() {
    }

    public static NodeMetricsService create() {
        return new NodeMetricsService(new QosMetrics(), new BackpressureMetrics(),
                new ConnectionRegistry(), new SessionStoreService(), new SubscribeStoreService(),
                TestObjectProviders.empty(), TestObjectProviders.empty(),
                TestBrokerProperties.create());
    }
}
