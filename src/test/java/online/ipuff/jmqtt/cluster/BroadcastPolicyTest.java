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
import online.ipuff.jmqtt.cluster.kafka.KafkaClusterBus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 集群广播策略。
 *
 * <h2>为什么这个开关需要专门的测试</h2>
 * 「广播关掉」的效果是<b>静默</b>的: 消息不会跨节点, 但不会报错、不会打日志、
 * 发布方与订阅方都感觉不到异常。也就是说, 一旦策略判断写反了
 * (例如把「未配置过滤器」当成「什么都不广播」, 或者通配符匹配退化成字符串前缀比较),
 * 测试之外几乎没有办法发现。
 *
 * <p>所以这里的用例集中在<b>边界</b>而不是主路径: 关闭 / 全部 / 过滤 三种形态的区分,
 * 通配符匹配是否真的按 MQTT 语义走, 以及「接管通道是否被误伤」——
 * 最后这条最要紧: 接管是会话正确性的一环, 它不能被一个「省流量」的开关波及。
 */
class BroadcastPolicyTest {

    // ------------------------------------------------------------------
    // 配置语义
    // ------------------------------------------------------------------

    @Test
    @DisplayName("未配置过滤器 = 全部广播(而不是什么都不广播)")
    void emptyFiltersMeansBroadcastAll() {
        assertTrue(kafka(true, List.of()).broadcastAll(),
                "空列表语义必须是「全部」—— 若被理解成「都不广播」, 漏配一个字段就会静默丢掉所有跨节点消息");
        assertTrue(kafka(true, null).broadcastAll(), "null 同样视为未配置");
        assertFalse(kafka(true, List.of("a/b")).broadcastAll(), "配了过滤器就不再是全部");
    }

    // ------------------------------------------------------------------
    // 总线实现
    // ------------------------------------------------------------------

    @Test
    @DisplayName("广播关闭: 所有主题都被拒绝, 且计入 broadcastSkipped")
    void broadcastOffDeniesEverything() {
        KafkaClusterBus bus = newBus(kafka(false, List.of()));

        assertFalse(bus.isBroadcastAllowed("sensor/room1/temp"));
        assertFalse(bus.isBroadcastAllowed("app/123/cmd"));

        // 这个计数是操作者唯一能判断「开关省下了多少」的口径
        assertEquals(2L, bus.stats().get("broadcastSkipped"));
    }

    @Test
    @DisplayName("未配置过滤器: 全部放行, 不产生任何跳过计数")
    void broadcastAllAllowsEverything() {
        KafkaClusterBus bus = newBus(kafka(true, List.of()));

        assertTrue(bus.isBroadcastAllowed("sensor/room1/temp"));
        assertTrue(bus.isBroadcastAllowed("$SYS/broker/uptime"));
        assertTrue(bus.isBroadcastAllowed("任意/中文/主题"));

        assertEquals(0L, bus.stats().get("broadcastSkipped"));
    }

    @Test
    @DisplayName("按过滤器收敛: 只有命中的主题跨节点")
    void broadcastFiltersApplyWildcards() {
        KafkaClusterBus bus = newBus(kafka(true, List.of("app/+/cmd", "device/+/signal")));

        assertTrue(bus.isBroadcastAllowed("app/123/cmd"), "命中 + 通配符");
        assertTrue(bus.isBroadcastAllowed("device/abc/signal"), "命中 + 通配符");
        assertFalse(bus.isBroadcastAllowed("sensor/room1/temp"), "遥测不在白名单里, 不该跨节点");
        assertFalse(bus.isBroadcastAllowed("app/123/status"), "前缀相同但不是同一主题");

        assertEquals(2L, bus.stats().get("broadcastSkipped"));
    }

    @Test
    @DisplayName("过滤器全非法时退化为全部广播, 而不是全部拒绝")
    void invalidFiltersFallBackToAll() {
        // sport/tennis# 是非法过滤器(# 未独立占层), "" 也是空的
        KafkaClusterBus bus = newBus(kafka(true, List.of("sport/tennis#", "")));

        // 「配置写错了」与「故意关掉广播」必须导向相反的结果:
        // 前者应当退化成安全的一侧(照常广播), 后者才是不广播。
        // 否则一次手误就会变成「跨节点消息静默消失」。
        assertTrue(bus.isBroadcastAllowed("sensor/room1/temp"));
    }

    @Test
    @DisplayName("接管通道不受广播开关影响")
    void takeoverIsNotGatedByBroadcast() {
        KafkaClusterBus bus = newBus(kafka(false, List.of()));

        assertFalse(bus.isBroadcastAllowed("app/123/cmd"), "消息面确实被关掉了");

        // 但接管必须照常出站: 它承载的是「同一 clientId 的旧连接该释放了」,
        // 属于会话正确性的一部分, 与「跨节点 pub/sub 要不要做」是两个问题。
        // 一旦被这个开关误伤, 表现为同一 clientId 在两个节点同时在线,
        // 而这在业务侧看是「指令被处理了两次」这种极难定位的现象。
        bus.publishTakeover("client-1", "node-b");

        assertEquals(1L, bus.stats().get("outboxSize"),
                "接管记录必须进入出站队列, 不受 broadcast-enabled 影响");
    }

    // ------------------------------------------------------------------

    private static KafkaClusterBus newBus(BrokerProperties.KafkaProperties kafka) {
        BrokerProperties properties = new BrokerProperties(
                "node-1", null, 1883, true, 8083, "/mqtt",
                1, 2, false, 4096,
                false, "jmqtt", "jmqtt",
                60, 7200, 0, 0, 32, 1000, 1000,
                true, kafka,
                new BrokerProperties.RedisProperties(false, "127.0.0.1", 6379, null, 0,
                        "jmqtt", 1000, 5000, "off", 100, 10000, "standalone", null, null),
                511, true, true, 10485760, 32768, 65536);
        // 不调用 start(): 本组用例只验证策略判断, 不需要连接 Kafka。
        // internalSendServer / takeoverListener 在这条路径上不会被触达。
        return new KafkaClusterBus(properties, null, (clientId, fromNodeId) -> {
        });
    }

    /** 与 ClusterBusTest 的同类辅助保持一致, 便于两个文件对照阅读 */
    private static BrokerProperties.KafkaProperties kafka(boolean broadcastEnabled,
                                                          List<String> broadcastFilters) {
        return new BrokerProperties.KafkaProperties(
                true,
                "127.0.0.1:9092",
                "jmqtt",
                "jmqtt-cluster",
                null,
                broadcastEnabled,
                broadcastFilters,
                "",
                List.of(),
                false,
                1000,
                1000,
                200,
                1,
                "none",
                "latest",
                "topic",
                null, null, null);
    }
}
