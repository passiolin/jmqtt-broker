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

import online.ipuff.jmqtt.store.InflightPersistence;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Redis 已启用但<b>连不上</b>时的启动冒烟测试。
 *
 * <h2>验证的是哪条承诺</h2>
 * 设计上反复强调: 「Redis 挂掉不应该导致 broker 起不来」——
 * 连接是惰性的, 只在首次使用时建立, 失败则降级, 恢复后自动重新生效。
 * 但这条承诺只有在「配置启用了 Redis 而 Redis 真的不可达」时才会被检验,
 * 平时的用例都是连得上 Redis 的, 覆盖不到。
 *
 * <p>这里把 {@code redis.enabled} 打开, 端口指向一个<b>确定没有服务在监听</b>的位置,
 * 然后要求上下文照常 refresh 完成。同时断言 {@link InflightPersistence}
 * 在这种情况下仍然是可注入的 Bean(而不是因为条件装配而缺失) ——
 * 后者正是曾经让默认配置启动失败的那个坑的另一种形态。
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "jmqtt.broker.port=21884",
                "jmqtt.broker.websocket-enabled=false",
                "jmqtt.broker.redis.enabled=true",
                // 21999 上不会有 Redis, 用来模拟「启用了但连不上」
                "jmqtt.broker.redis.port=21999",
                "jmqtt.broker.redis.command-timeout-ms=200",
                "jmqtt.broker.redis.health-interval-ms=60000",
                // application.yml 默认是 async, 这里显式指定以便断言 mode() 的读取路径
                "jmqtt.broker.redis.inflight-mode=sync"
        })
class BrokerStartupRedisUnreachableTest {

    @Autowired
    private InflightPersistence inflightPersistence;

    @Test
    @DisplayName("Redis 启用了但连不上时, 上下文仍能启动且持久化组件可注入")
    void contextLoadsWhenRedisUnreachable() {
        // Bean 存在 —— 曾经它因为条件装配而在禁用 Redis 时缺失, 直接导致启动失败
        assertNotNull(inflightPersistence, "InflightPersistence 应为始终存在的 Bean");
        // 配置读得出来
        assertEquals(InflightPersistence.Mode.SYNC, inflightPersistence.mode(),
                "mode() 应反映配置里的 inflight-mode");
        // 关键: 读不到时必须优雅降级成空结果, 而不是把异常抛到 MQTT 处理路径上
        assertTrue(inflightPersistence.load("no-such-client").isEmpty(),
                "Redis 不可达时 load 应返回空列表而不是抛异常");
    }
}
