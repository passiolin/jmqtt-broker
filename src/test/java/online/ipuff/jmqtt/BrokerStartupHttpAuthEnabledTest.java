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

import online.ipuff.jmqtt.auth.HttpAuthService;
import online.ipuff.jmqtt.auth.IAuthService;
import online.ipuff.jmqtt.authz.HttpAclService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * HTTP 认证/ACL 启用(但服务不可达)时的启动冒烟测试。
 *
 * <p>与 Redis 版冒烟同一条承诺: <b>外部依赖连不上不应该导致 broker 起不来</b>。
 * 认证/ACL 客户端都是惰性连接 —— 启动时只构造客户端, 不发请求;
 * 真正的故障路径是连接到来时按 on-error/on-fail 结束。
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "jmqtt.broker.port=21885",
                "jmqtt.broker.websocket-enabled=false",
                "jmqtt.broker.http-auth.enabled=true",
                // 1 号端口上不会有 HTTP 服务, 模拟「启用了但连不上」
                "jmqtt.broker.http-auth.url=http://127.0.0.1:1/auth",
                "jmqtt.broker.http-auth.request-timeout-ms=200",
                "jmqtt.broker.http-acl.enabled=true",
                "jmqtt.broker.http-acl.url=http://127.0.0.1:1/acl",
                "jmqtt.broker.http-acl.request-timeout-ms=200",
                "jmqtt.broker.http-acl.cooldown-ms=60000"
        })
class BrokerStartupHttpAuthEnabledTest {

    @Autowired
    private IAuthService authService;

    @Autowired
    private HttpAclService aclService;

    @Test
    @DisplayName("认证/ACL 服务不可达时上下文仍能启动, 且故障路径收敛为拒绝")
    void contextLoadsWhenAuthServerUnreachable() {
        assertNotNull(authService, "IAuthService 应被装配(HTTP 模式)");
        assertEquals(HttpAuthService.class, authService.getClass(),
                "http-auth.enabled=true 时应装配 HTTP 认证实现");

        // 认证服务不可达: on-error 默认 reject —— fail-closed, 而不是异常/挂起
        assertFalse(authService.authenticate("c1", "u", "p", "1.2.3.4").join().allowed(),
                "认证服务不可达时必须拒绝(默认 fail-closed)");

        // ACL 服务不可达: on-fail 默认 deny, future 收敛而不是异常
        assertFalse(aclService.check("c1", "u", "1.2.3.4", "publish", "t/1").join(),
                "ACL 服务不可达时必须拒绝");
        assertTrue(aclService.check("c1", "u", "1.2.3.4", "publish", "t/1").isDone(),
                "故障决策必须已完成(冷却期内短路)");
    }
}
