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
package online.ipuff.jmqtt;

import online.ipuff.jmqtt.config.BrokerProperties;

import java.util.List;

/**
 * 测试用的默认配置。
 *
 * <p>存在的理由很实际: {@link BrokerProperties} 是构造器绑定的 record, 加一个配置项
 * 就会让<b>每一处手工构造它的测试</b>一起编译失败。之前这份构造代码在三个测试类里
 * 各写了一遍, 于是每加一个字段就要改三处、还漏过一次。
 * 收在这里之后, 新增配置只需要改这一个地方。
 */
public final class TestBrokerProperties {

    private TestBrokerProperties() {
    }

    /** 一个单机、无 Redis、无集群的最小配置 */
    public static BrokerProperties create() {
        return create(32, 1000);
    }

    /** 指定认证配置的最小配置(认证挂起/HTTP 鉴权测试用) */
    public static BrokerProperties create(boolean authEnabled, String authUsername, String authPassword) {
        return create("node-1", 32, 1000,
                new BrokerProperties.KafkaProperties(
                        false, "127.0.0.1:9092", "jmqtt", "jmqtt-cluster", null,
                        true, List.of(),
                        "", List.of(), false, 1000, 1000, 200, 1, "none", "latest", "topic", null, null, null),
                authEnabled, authUsername, authPassword);
    }

    /**
     * @param maxInflight       单客户端在途窗口; 同时决定 QoS 2 去重状态的每客户端上限
     * @param maxOfflineQueueLen 离线队列长度
     */
    public static BrokerProperties create(int maxInflight, int maxOfflineQueueLen) {
        return create("node-1", maxInflight, maxOfflineQueueLen,
                new BrokerProperties.KafkaProperties(
                        false, "127.0.0.1:9092", "jmqtt", "jmqtt-cluster", null,
                        true, List.of(),
                        "", List.of(), false, 1000, 1000, 200, 1, "none", "latest", "topic", null, null, null));
    }

    public static BrokerProperties create(String nodeId, int maxInflight, int maxOfflineQueueLen,
                                          BrokerProperties.KafkaProperties kafka) {
        return create(nodeId, maxInflight, maxOfflineQueueLen, kafka, false, "jmqtt", "jmqtt");
    }

    public static BrokerProperties create(String nodeId, int maxInflight, int maxOfflineQueueLen,
                                          BrokerProperties.KafkaProperties kafka,
                                          boolean authEnabled, String authUsername, String authPassword) {
        BrokerProperties.RedisProperties redis = new BrokerProperties.RedisProperties(
                false, "127.0.0.1", 6379, null, 0, "jmqtt", 1000, 5000, "off", 100, 10000, "standalone", null, null);
        return new BrokerProperties(
                nodeId, null, 1883, true, 8083, "/mqtt",
                1, 2, false,
                authEnabled, authUsername, authPassword,
                60, 7200, 0, 0, maxInflight, 1000, maxOfflineQueueLen,
                false, kafka, redis,
                511, true, true, 10485760, 32768, 65536);
    }
}
