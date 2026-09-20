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
package online.ipuff.jmqtt.auth;

import online.ipuff.jmqtt.TestBrokerProperties;
import online.ipuff.jmqtt.config.BrokerProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 内置静态鉴权(用户名/密码来自 application.yml)的异步接口测试。
 *
 * <p>重点钉住两件事: 判定逻辑本身(明文/sha256/常量时间比较在 {@link AuthService} 里,
 * 这里只测行为), 以及<b>future 必须立即完成</b> —— 内置鉴权没有外部调用,
 * 若返回未完成的 future, 说明有人在认证路径上引入了不必要的异步。
 */
class AuthServiceTest {

    private static final String SHA256_OF_JMQTT = AuthService.sha256Hex("jmqtt");

    private AuthService service(boolean authEnabled, String username, String password) {
        return new AuthService(TestBrokerProperties.create(authEnabled, username, password));
    }

    private static AuthResult await(CompletableFuture<AuthResult> future) {
        try {
            return future.get(2, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("内置鉴权的 future 必须立即完成", e);
        }
    }

    @Test
    @DisplayName("auth-enabled=false 直接放行, 不看凭据")
    void disabledAllowsEverything() {
        CompletableFuture<AuthResult> future =
                service(false, "jmqtt", "jmqtt").authenticate("c1", "whoever", "whatever", "1.2.3.4");
        assertTrue(future.isDone(), "关闭时必须零开销同步完成");
        assertTrue(await(future).allowed());
    }

    @Test
    @DisplayName("正确的明文凭据放行")
    void correctPlainCredentialsAllowed() {
        AuthResult result = await(service(true, "jmqtt", "jmqtt")
                .authenticate("c1", "jmqtt", "jmqtt", "1.2.3.4"));
        assertTrue(result.allowed());
        assertFalse(result.superuser(), "内置鉴权没有 superuser 概念");
    }

    @Test
    @DisplayName("密码错误拒绝")
    void wrongPasswordDenied() {
        assertFalse(await(service(true, "jmqtt", "jmqtt")
                .authenticate("c1", "jmqtt", "wrong", "1.2.3.4")).allowed());
    }

    @Test
    @DisplayName("用户名错误拒绝")
    void wrongUsernameDenied() {
        assertFalse(await(service(true, "jmqtt", "jmqtt")
                .authenticate("c1", "other", "jmqtt", "1.2.3.4")).allowed());
    }

    @Test
    @DisplayName("用户名或密码缺失拒绝")
    void missingCredentialsDenied() {
        AuthService service = service(true, "jmqtt", "jmqtt");
        assertFalse(await(service.authenticate("c1", null, "jmqtt", "1.2.3.4")).allowed());
        assertFalse(await(service.authenticate("c1", "jmqtt", null, "1.2.3.4")).allowed());
        assertFalse(await(service.authenticate("c1", "", "jmqtt", "1.2.3.4")).allowed());
    }

    @Test
    @DisplayName("sha256: 形式的配置密码可用明文提交校验")
    void sha256ConfiguredPasswordAccepted() {
        AuthService service = service(true, "jmqtt", "sha256:" + SHA256_OF_JMQTT);
        assertTrue(await(service.authenticate("c1", "jmqtt", "jmqtt", "1.2.3.4")).allowed());
        assertFalse(await(service.authenticate("c1", "jmqtt", "wrong", "1.2.3.4")).allowed());
    }

    @Test
    @DisplayName("未配置密码时拒绝而不是放行")
    void missingPasswordConfigDenied() {
        assertFalse(await(service(true, "jmqtt", null)
                .authenticate("c1", "jmqtt", "jmqtt", "1.2.3.4")).allowed());
    }

    @Test
    @DisplayName("clientId 与 peerhost 不参与内置鉴权判定")
    void clientIdAndPeerHostIgnored() {
        AuthService service = service(true, "jmqtt", "jmqtt");
        assertTrue(await(service.authenticate("any-client", "jmqtt", "jmqtt", null)).allowed());
        assertNotNull(service, "保持引用, 避免误删构造");
    }
}
