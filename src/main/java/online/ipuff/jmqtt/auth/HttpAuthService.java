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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * HTTP 认证服务(EMQX 5.x HTTP authenticator 契约)。
 *
 * <p>请求: POST {@code {"clientid","username","password","peerhost"}}(password 按
 * {@code password-hash} 配置以明文或 sha256 摘要发送; GET 方式走查询参数)。
 * 响应: 200 + {@code {"result":"allow"|"deny"|"ignore","superuser":bool?}}。
 *
 * <p><b>认证链</b>: {@code ignore} 与「超时/不可达/响应不合法」(on-error=ignore 时)
 * 都回落到构造时传入的下一环(内置静态鉴权)—— 与 EMQX 认证器链的 ignore 语义一致:
 * 「本认证器不管这件事, 交给下一个」。
 *
 * <p><b>熔断冷却</b>: 一次传输层失败(超时/IO/非 200/坏响应)后进入 cooldown-ms 冷却期,
 * 期间不再发请求、直接按 on-error 结束 —— 认证中心故障不该让每个 CONNECT 都吃满超时。
 * 任何一次成功响应(含 deny —— deny 是明确决策, 不是故障)都会结束冷却。
 *
 * <p>future 永不异常完成: 任何意外都收敛为 deny / 回落, 认证路径上没有「炸出去」的路径。
 */
public class HttpAuthService implements IAuthService {

    private static final Logger log = LoggerFactory.getLogger(HttpAuthService.class);

    private final HttpAuthProperties properties;
    private final IAuthService fallback;
    private final HttpClient httpClient;
    private final ObjectMapper mapper = new ObjectMapper();
    /** 熔断冷却截止时刻(epoch millis); 0 表示不在冷却期 */
    private final AtomicLong cooldownUntil = new AtomicLong();

    public HttpAuthService(HttpAuthProperties properties, IAuthService fallback) {
        if (properties.url() == null || properties.url().isBlank()) {
            throw new IllegalStateException(
                    "jmqtt.broker.http-auth.enabled=true 但未配置 url —— 拒绝启动而不是静默放行。"
                            + "请配置 http-auth.url, 或将 http-auth.enabled 置为 false。");
        }
        this.properties = properties;
        this.fallback = fallback;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(properties.connectTimeoutMs()))
                .build();
    }

    @Override
    public CompletableFuture<AuthResult> authenticate(String clientId, String username,
                                                      String password, String peerhost) {
        long now = System.currentTimeMillis();
        long until = cooldownUntil.get();
        if (until > now) {
            log.debug("认证服务冷却期中(剩余 {}ms), 按 on-error={} 处理", until - now, properties.onError());
            return onFallback(username, password);
        }

        String sentPassword = "sha256".equalsIgnoreCase(properties.passwordHash()) && password != null
                ? AuthService.sha256Hex(password)
                : password;

        HttpRequest request = properties.useGetMethod()
                ? getRequest(clientId, username, sentPassword, peerhost)
                : postRequest(clientId, username, sentPassword, peerhost);

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .orTimeout(properties.requestTimeoutMs(), java.util.concurrent.TimeUnit.MILLISECONDS)
                .thenApply(response -> parse(response, clientId, username, password))
                .exceptionally(e -> {
                    enterCooldown(now);
                    log.warn("认证服务请求失败(进入 {}ms 冷却): {}",
                            properties.cooldownMs(), e.getMessage());
                    return join(onFallback(username, password));
                });
    }

    private HttpRequest postRequest(String clientId, String username, String password, String peerhost) {
        String json;
        try {
            json = mapper.writeValueAsString(java.util.Map.of(
                    "clientid", nullSafe(clientId),
                    "username", nullSafe(username),
                    "password", nullSafe(password),
                    "peerhost", nullSafe(peerhost)));
        } catch (Exception e) {
            throw new IllegalStateException("认证请求序列化失败", e);
        }
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(properties.url()))
                .timeout(Duration.ofMillis(properties.requestTimeoutMs()))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8));
        applyHeaders(builder);
        return builder.build();
    }

    private HttpRequest getRequest(String clientId, String username, String password, String peerhost) {
        String query = "clientid=" + encode(clientId)
                + "&username=" + encode(username)
                + "&password=" + encode(password)
                + "&peerhost=" + encode(peerhost);
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(properties.url() + "?" + query))
                .timeout(Duration.ofMillis(properties.requestTimeoutMs()))
                .GET();
        applyHeaders(builder);
        return builder.build();
    }

    private void applyHeaders(HttpRequest.Builder builder) {
        if (properties.headers() != null) {
            properties.headers().forEach(builder::header);
        }
    }

    /**
     * 解析认证服务响应。非 200 / 坏 JSON / 未知 result 都按故障处理(熔断 + on-error)。
     */
    private AuthResult parse(HttpResponse<String> response, String clientId,
                             String username, String password) {
        if (response.statusCode() != 200) {
            return transportFailure("认证服务 HTTP " + response.statusCode(), username, password);
        }
        JsonNode root;
        try {
            root = mapper.readTree(response.body());
        } catch (Exception e) {
            return transportFailure("认证服务响应不是合法 JSON", username, password);
        }
        String result = root.path("result").asText("");
        boolean superuser = root.path("superuser").asBoolean(false);
        switch (result) {
            case "allow":
                cooldownUntil.set(0);
                return new AuthResult(true, superuser);
            case "deny":
                cooldownUntil.set(0);
                return AuthResult.DENY;
            case "ignore":
                // ignore 是认证链语义(本认证器不管, 交给下一环), 与 on-error 策略无关
                return join(chainFallthrough(username, password));
            default:
                return transportFailure("认证服务返回未知 result: " + result, username, password);
        }
    }

    private AuthResult transportFailure(String reason, String username, String password) {
        enterCooldown(System.currentTimeMillis());
        log.warn("{}(进入 {}ms 冷却)", reason, properties.cooldownMs());
        return join(onFallback(username, password));
    }

    /**
     * 认证链回落(EMQX ignore 语义): 交给下一环(内置静态鉴权)判定。
     */
    private CompletableFuture<AuthResult> chainFallthrough(String username, String password) {
        // 内置鉴权不看 clientId/peerhost, 传 null 即可
        return fallback.authenticate(null, username, password, null);
    }

    /**
     * on-error 策略: 认证服务故障(超时/不可达/响应不合法)时,
     * reject(默认 fail-closed)或 ignore(回落认证链下一环)。
     */
    private CompletableFuture<AuthResult> onFallback(String username, String password) {
        if (!properties.fallbackOnIgnore()) {
            return CompletableFuture.completedFuture(AuthResult.DENY);
        }
        return chainFallthrough(username, password);
    }

    private void enterCooldown(long fromMs) {
        if (properties.cooldownMs() > 0) {
            cooldownUntil.set(fromMs + properties.cooldownMs());
        }
    }

    private static String nullSafe(String s) {
        return s == null ? "" : s;
    }

    private static String encode(String s) {
        return URLEncoder.encode(nullSafe(s), StandardCharsets.UTF_8);
    }

    private static AuthResult join(CompletableFuture<AuthResult> future) {
        try {
            return future.join();
        } catch (RuntimeException e) {
            return AuthResult.DENY;
        }
    }
}
