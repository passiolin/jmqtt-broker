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

import com.sun.net.httpserver.HttpServer;
import online.ipuff.jmqtt.TestBrokerProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * HTTP 认证服务(EMQX 契约)的行为测试。
 *
 * <p>用 JDK 内置 {@link HttpServer} 起一个<b>真实的</b>认证服务端 —— 项目风格是无 mock,
 * 这里断言的是线上格式(JSON 字段、状态码语义)而不是「调用过谁」。
 *
 * <p>契约: POST JSON {@code {"clientid","username","password","peerhost"}} →
 * 200 + {@code {"result":"allow"|"deny"|"ignore","superuser":bool?}}。
 * ignore / 错误 / 超时按 on-error 策略(reject 或回落到内置静态鉴权)。
 */
class HttpAuthServiceTest {

    /** 可控的认证服务端: 响应体/状态码/延迟可变, 并统计请求数 */
    private HttpServer server;
    private final AtomicInteger hits = new AtomicInteger();
    private volatile int status = 200;
    private volatile String body = "{\"result\":\"allow\"}";
    private volatile long delayMs = 0;
    private volatile String lastRequestBody = "";
    private volatile String lastRequestQuery = "";

    private HttpAuthService service;
    private IAuthService internal;

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/auth", exchange -> {
            hits.incrementAndGet();
            if (delayMs > 0) {
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
            lastRequestBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            lastRequestQuery = exchange.getRequestURI().getRawQuery();
            byte[] out = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, out.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(out);
            }
        });
        server.start();
        // 内置静态鉴权(jmqtt/jmqtt)作为认证链的下一环
        internal = new AuthService(TestBrokerProperties.create(true, "jmqtt", "jmqtt"));
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    private HttpAuthService service(String method, String passwordHash, String onError, long cooldownMs) {
        HttpAuthProperties properties = new HttpAuthProperties(true, url(method), method,
                500, 500, passwordHash, onError, cooldownMs, Map.of("X-Trace", "t1"));
        return new HttpAuthService(properties, internal);
    }

    private String url(String method) {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/auth";
    }

    private static AuthResult await(CompletableFuture<AuthResult> future) {
        try {
            return future.get(3, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("认证 future 未在预期时间内完成", e);
        }
    }

    @Test
    @DisplayName("result=allow 放行且默认非 superuser")
    void allowResult() {
        body = "{\"result\":\"allow\"}";
        AuthResult result = await(service("post", "plain", "reject", 0)
                .authenticate("c1", "jmqtt", "secret", "10.0.0.1"));
        assertTrue(result.allowed());
        assertFalse(result.superuser());
        assertEquals(1, hits.get());
        assertTrue(lastRequestBody.contains("\"clientid\":\"c1\""), "EMQX 契约字段 clientid: " + lastRequestBody);
        assertTrue(lastRequestBody.contains("\"username\":\"jmqtt\""));
        assertTrue(lastRequestBody.contains("\"password\":\"secret\""), "默认 plain 传明文");
        assertTrue(lastRequestBody.contains("\"peerhost\":\"10.0.0.1\""));
    }

    @Test
    @DisplayName("result=allow 且 superuser=true 时授予超级用户")
    void superuserGranted() {
        body = "{\"result\":\"allow\",\"superuser\":true}";
        AuthResult result = await(service("post", "plain", "reject", 0)
                .authenticate("c1", "jmqtt", "secret", "10.0.0.1"));
        assertTrue(result.allowed());
        assertTrue(result.superuser());
    }

    @Test
    @DisplayName("result=deny 拒绝, 不回落")
    void denyResult() {
        body = "{\"result\":\"deny\"}";
        AuthResult result = await(service("post", "plain", "reject", 0)
                .authenticate("c1", "jmqtt", "secret", "10.0.0.1"));
        assertFalse(result.allowed());
    }

    @Test
    @DisplayName("result=ignore 回落到内置静态鉴权: 凭据正确则放行")
    void ignoreFallsBackToInternalAllow() {
        body = "{\"result\":\"ignore\"}";
        AuthResult result = await(service("post", "plain", "reject", 0)
                .authenticate("c1", "jmqtt", "jmqtt", "10.0.0.1"));
        assertTrue(result.allowed(), "ignore = 交给认证链下一环(内置 jmqtt/jmqtt)");
    }

    @Test
    @DisplayName("result=ignore 回落到内置静态鉴权: 凭据错误则拒绝")
    void ignoreFallsBackToInternalDeny() {
        body = "{\"result\":\"ignore\"}";
        AuthResult result = await(service("post", "plain", "reject", 0)
                .authenticate("c1", "jmqtt", "wrong", "10.0.0.1"));
        assertFalse(result.allowed());
    }

    @Test
    @DisplayName("认证服务超时: on-error=reject 时拒绝并进入熔断冷却")
    void timeoutRejectsAndEntersCooldown() {
        delayMs = 5000; // 服务端卡死, 客户端 500ms 超时
        HttpAuthService svc = service("post", "plain", "reject", 60_000);
        long start = System.nanoTime();
        AuthResult first = await(svc.authenticate("c1", "jmqtt", "secret", "10.0.0.1"));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertFalse(first.allowed());
        assertTrue(elapsedMs < 2500, "必须按 request-timeout 失败而不是等服务端, 实际 " + elapsedMs + "ms");
        assertEquals(1, hits.get());

        // 冷却期内: 同一实例不再发请求, 立即按策略拒绝
        start = System.nanoTime();
        AuthResult second = await(svc.authenticate("c2", "jmqtt", "secret", "10.0.0.1"));
        elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertFalse(second.allowed());
        assertTrue(elapsedMs < 300, "冷却期内必须短路, 实际 " + elapsedMs + "ms");
    }

    @Test
    @DisplayName("认证服务 5xx: on-error=ignore 时回落到内置静态鉴权")
    void serverErrorFallsBackWhenConfigured() {
        status = 500;
        body = "boom";
        AuthResult result = await(service("post", "plain", "ignore", 60_000)
                .authenticate("c1", "jmqtt", "jmqtt", "10.0.0.1"));
        assertTrue(result.allowed(), "ignore 策略下认证服务故障回落到内置凭据");
    }

    @Test
    @DisplayName("认证服务 5xx: on-error=reject(默认)时拒绝")
    void serverErrorRejectsByDefault() {
        status = 503;
        body = "";
        assertFalse(await(service("post", "plain", "reject", 0)
                .authenticate("c1", "jmqtt", "jmqtt", "10.0.0.1")).allowed());
    }

    @Test
    @DisplayName("200 但响应体不是合法 JSON: 按错误处理")
    void malformedBodyTreatedAsError() {
        body = "not-json-at-all";
        assertFalse(await(service("post", "plain", "reject", 0)
                .authenticate("c1", "jmqtt", "jmqtt", "10.0.0.1")).allowed());
    }

    @Test
    @DisplayName("未知 result 值: 按错误处理而不是放行")
    void unknownResultTreatedAsError() {
        body = "{\"result\":\"maybe\"}";
        assertFalse(await(service("post", "plain", "reject", 0)
                .authenticate("c1", "jmqtt", "jmqtt", "10.0.0.1")).allowed());
    }

    @Test
    @DisplayName("password-hash=sha256: 请求体里是摘要而不是明文")
    void sha256PasswordHashSent() {
        body = "{\"result\":\"allow\"}";
        await(service("post", "sha256", "reject", 0)
                .authenticate("c1", "jmqtt", "secret", "10.0.0.1"));
        String expected = AuthService.sha256Hex("secret");
        assertTrue(lastRequestBody.contains(expected), "应传 sha256 摘要: " + lastRequestBody);
        assertFalse(lastRequestBody.contains("\"password\":\"secret\""), "不得出现明文密码");
    }

    @Test
    @DisplayName("GET 方式: 凭据走查询参数")
    void getMethodUsesQueryParams() {
        body = "{\"result\":\"allow\"}";
        await(service("get", "plain", "reject", 0)
                .authenticate("c1", "jmqtt", "secret", "10.0.0.1"));
        assertTrue(lastRequestQuery.contains("username=jmqtt"), "查询参数: " + lastRequestQuery);
        assertTrue(lastRequestQuery.contains("password=secret"));
        assertTrue(lastRequestQuery.contains("clientid=c1"));
    }

    @Test
    @DisplayName("认证服务不可达: 冷却期内短路, on-error=ignore 回落内置")
    void unreachableEntersCooldown() {
        status = 500;
        HttpAuthService svc = service("post", "plain", "ignore", 60_000);
        assertTrue(await(svc.authenticate("c1", "jmqtt", "jmqtt", "10.0.0.1")).allowed());
        assertEquals(1, hits.get());
        // 冷却期内第二次: 不打服务端
        assertTrue(await(svc.authenticate("c2", "jmqtt", "jmqtt", "10.0.0.1")).allowed());
        assertEquals(1, hits.get(), "冷却期内不得再发请求");
    }

    @Test
    @DisplayName("enabled=true 但 url 为空: 构造即失败(fail-fast)")
    void blankUrlFailsFast() {
        HttpAuthProperties bad = new HttpAuthProperties(true, "", "post",
                500, 500, "plain", "reject", 0, Map.of());
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> new HttpAuthService(bad, internal));
    }
}
