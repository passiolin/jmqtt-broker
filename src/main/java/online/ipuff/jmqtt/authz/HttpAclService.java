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
package online.ipuff.jmqtt.authz;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * HTTP ACL 服务(EMQX 5.x HTTP authorization 契约)。
 *
 * <p>请求: POST {@code {"clientid","username","peerhost","action","topic"}}(action 为
 * {@code publish} 或 {@code subscribe}); 响应: 200 +
 * {@code {"result":"allow"|"deny"|"ignore"}}。ignore 与一切故障按 {@code on-fail} 结束。
 *
 * <p><b>决策缓存</b>(client+action+topic → allow/deny, TTL + 每客户端 LRU 上限)是本服务
 * 的核心而非优化: ACL 检查挂在每条 PUBLISH 上, 没有缓存等于把投递路径接进 HTTP RTT。
 * 故障期间的决策(on-fail 产物)<b>不进缓存</b> —— 服务恢复后必须重新判定。
 *
 * <p>superuser 判定不在本类: 认证时已把 {@code SUPERUSER} 标志写到 Channel 上,
 * 调用方(SubscribeHandler/PublishHandler)先查标志、命中即根本不进来。
 */
@Component
public class HttpAclService implements IAclService {

    private static final Logger log = LoggerFactory.getLogger(HttpAclService.class);

    private final HttpAclProperties properties;
    private final HttpClient httpClient;
    private final ObjectMapper mapper = new ObjectMapper();
    /** clientId → 该客户端的决策缓存; 断连时整体移除 */
    private final ConcurrentHashMap<String, ClientCache> caches = new ConcurrentHashMap<>();
    /** 熔断冷却截止时刻(epoch millis); 0 表示不在冷却期 */
    private final AtomicLong cooldownUntil = new AtomicLong();

    public HttpAclService(HttpAclProperties properties) {
        if (properties.enabled() && (properties.url() == null || properties.url().isBlank())) {
            throw new IllegalStateException(
                    "jmqtt.broker.http-acl.enabled=true 但未配置 url —— 拒绝启动而不是静默放行。"
                            + "请配置 http-acl.url, 或将 http-acl.enabled 置为 false。");
        }
        this.properties = properties;
        this.httpClient = properties.enabled()
                ? HttpClient.newBuilder()
                        .connectTimeout(Duration.ofMillis(properties.connectTimeoutMs()))
                        .build()
                : null;
    }

    /**
     * 是否放行一次 publish/subscribe。
     *
     * <p>关闭时恒为 allow 且<b>同步完成</b> —— 调用方据此走零开销内联路径。
     */
    @Override
    public CompletableFuture<Boolean> check(String clientId, String username,
                                            String peerhost, String action, String topic) {
        if (!properties.enabled()) {
            return CompletableFuture.completedFuture(Boolean.TRUE);
        }
        String cacheKey = action + "\n" + topic;
        ClientCache cache = caches.get(clientId);
        if (cache != null) {
            Boolean cached = cache.get(cacheKey);
            if (cached != null) {
                return CompletableFuture.completedFuture(cached);
            }
        }

        long now = System.currentTimeMillis();
        long until = cooldownUntil.get();
        if (until > now) {
            log.debug("ACL 服务冷却期中(剩余 {}ms), 按 on-fail={} 处理", until - now, properties.onFail());
            return CompletableFuture.completedFuture(failOpen());
        }

        HttpRequest request = buildRequest(clientId, username, peerhost, action, topic);
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .orTimeout(properties.requestTimeoutMs(), TimeUnit.MILLISECONDS)
                .thenApply(response -> parse(response, clientId, cacheKey))
                .exceptionally(e -> {
                    enterCooldown(now);
                    log.warn("ACL 服务请求失败(进入 {}ms 冷却): {}", properties.cooldownMs(), e.getMessage());
                    return failOpen(); // 故障决策不进缓存
                });
    }

    /**
     * 客户端断连: 释放其决策缓存。由连接关闭路径调用。
     */
    @Override
    public void onClientOffline(String clientId) {
        caches.remove(clientId);
    }

    private HttpRequest buildRequest(String clientId, String username,
                                     String peerhost, String action, String topic) {
        if (properties.useGetMethod()) {
            String query = "clientid=" + encode(clientId)
                    + "&username=" + encode(username)
                    + "&peerhost=" + encode(peerhost)
                    + "&action=" + encode(action)
                    + "&topic=" + encode(topic);
            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(properties.url() + "?" + query))
                    .timeout(Duration.ofMillis(properties.requestTimeoutMs()))
                    .GET();
            applyHeaders(builder);
            return builder.build();
        }
        String json;
        try {
            json = mapper.writeValueAsString(Map.of(
                    "clientid", nullSafe(clientId),
                    "username", nullSafe(username),
                    "peerhost", nullSafe(peerhost),
                    "action", nullSafe(action),
                    "topic", nullSafe(topic)));
        } catch (Exception e) {
            throw new IllegalStateException("ACL 请求序列化失败", e);
        }
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(properties.url()))
                .timeout(Duration.ofMillis(properties.requestTimeoutMs()))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8));
        applyHeaders(builder);
        return builder.build();
    }

    /**
     * 解析响应。只有服务端的明确决策(allow/deny)才进缓存;
     * ignore 与一切故障按 on-fail 结束且不缓存。
     */
    private boolean parse(HttpResponse<String> response, String clientId, String cacheKey) {
        if (response.statusCode() != 200) {
            return transportFailure("ACL 服务 HTTP " + response.statusCode());
        }
        JsonNode root;
        try {
            root = mapper.readTree(response.body());
        } catch (Exception e) {
            return transportFailure("ACL 服务响应不是合法 JSON");
        }
        String result = root.path("result").asText("");
        switch (result) {
            case "allow":
                cooldownUntil.set(0);
                cacheDecision(clientId, cacheKey, true);
                return true;
            case "deny":
                cooldownUntil.set(0);
                cacheDecision(clientId, cacheKey, false);
                return false;
            case "ignore":
                log.debug("ACL 服务返回 ignore, 按 on-fail={} 处理", properties.onFail());
                return failOpen();
            default:
                return transportFailure("ACL 服务返回未知 result: " + result);
        }
    }

    private boolean transportFailure(String reason) {
        enterCooldown(System.currentTimeMillis());
        log.warn("{}(进入 {}ms 冷却)", reason, properties.cooldownMs());
        return failOpen();
    }

    private void cacheDecision(String clientId, String cacheKey, boolean allowed) {
        if (properties.cacheTtlSeconds() <= 0) {
            return;
        }
        caches.computeIfAbsent(clientId, c -> new ClientCache(properties.cacheMaxEntriesPerClient()))
                .put(cacheKey, allowed, System.currentTimeMillis() + properties.cacheTtlSeconds() * 1000);
    }

    private void enterCooldown(long fromMs) {
        if (properties.cooldownMs() > 0) {
            cooldownUntil.set(fromMs + properties.cooldownMs());
        }
    }

    private boolean failOpen() {
        return properties.failOpen();
    }

    private void applyHeaders(HttpRequest.Builder builder) {
        if (properties.headers() != null) {
            properties.headers().forEach(builder::header);
        }
    }

    private static String nullSafe(String s) {
        return s == null ? "" : s;
    }

    private static String encode(String s) {
        return URLEncoder.encode(nullSafe(s), StandardCharsets.UTF_8);
    }

    /**
     * 单客户端的决策缓存: 访问序 LRU + TTL 过期。
     * 条目少(默认 32), 临界区极短, 直接同步块足够。
     */
    private static final class ClientCache {

        private final int maxEntries;
        private final LinkedHashMap<String, long[]> entries;

        ClientCache(int maxEntries) {
            this.maxEntries = maxEntries;
            this.entries = new LinkedHashMap<>(16, 0.75f, true);
        }

        /** @return 缓存中的决策; 未命中或已过期返回 null */
        Boolean get(String key) {
            synchronized (entries) {
                long[] entry = entries.get(key);
                if (entry == null) {
                    return null;
                }
                if (System.currentTimeMillis() > entry[1]) {
                    entries.remove(key);
                    return null;
                }
                return entry[0] == 1L;
            }
        }

        void put(String key, boolean allowed, long expireAt) {
            synchronized (entries) {
                entries.put(key, new long[]{allowed ? 1L : 0L, expireAt});
                while (entries.size() > maxEntries) {
                    entries.remove(entries.keySet().iterator().next());
                }
            }
        }
    }
}
