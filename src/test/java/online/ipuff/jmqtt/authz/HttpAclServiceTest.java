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

import com.sun.net.httpserver.HttpServer;
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
 * HTTP ACL 服务(EMQX 授权契约)的行为测试: 发布/订阅前按
 * {@code {"clientid","username","peerhost","action","topic"}} 询问外部服务,
 * 响应 {@code {"result":"allow"|"deny"|"ignore"}}; ignore 与故障按 on-fail 策略。
 *
 * <p>缓存是本服务的存在理由 —— ACL 在每条 PUBLISH / 每个订阅过滤器上触发,
 * 没有缓存等于把投递路径接进 HTTP RTT。这里钉住: 命中不发请求、TTL 过期重查、
 * 单客户端条目上限(LRU)、断连清理、superuser 由调用方跳过(服务端无感知)。
 */
class HttpAclServiceTest {

    private HttpServer server;
    private final AtomicInteger hits = new AtomicInteger();
    private volatile int status = 200;
    private volatile String body = "{\"result\":\"allow\"}";
    private volatile String lastRequestBody = "";

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/acl", exchange -> {
            hits.incrementAndGet();
            lastRequestBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            byte[] out = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, out.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(out);
            }
        });
        server.start();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    private HttpAclService service(long cacheTtlSeconds, int cacheMaxEntries, String onFail, long cooldownMs) {
        HttpAclProperties properties = new HttpAclProperties(true, url(), "post",
                500, 500, cacheTtlSeconds, cacheMaxEntries, onFail, cooldownMs, Map.of());
        return new HttpAclService(properties);
    }

    private String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/acl";
    }

    private static boolean allow(HttpAclService service, String clientId, String action, String topic) {
        try {
            return service.check(clientId, "user", "10.0.0.1", action, topic)
                    .get(3, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("ACL future 未在预期时间内完成", e);
        }
    }

    @Test
    @DisplayName("allow 放行, 请求体携带 EMQX 契约字段")
    void allowCarriesContractFields() {
        assertTrue(allow(service(60, 32, "deny", 0), "c1", "publish", "a/b"));
        assertEquals(1, hits.get());
        assertTrue(lastRequestBody.contains("\"clientid\":\"c1\""), lastRequestBody);
        assertTrue(lastRequestBody.contains("\"action\":\"publish\""), lastRequestBody);
        assertTrue(lastRequestBody.contains("\"topic\":\"a/b\""), lastRequestBody);
        assertTrue(lastRequestBody.contains("\"username\":\"user\""), lastRequestBody);
        assertTrue(lastRequestBody.contains("\"peerhost\":\"10.0.0.1\""), lastRequestBody);
    }

    @Test
    @DisplayName("deny 拒绝")
    void denyRejected() {
        body = "{\"result\":\"deny\"}";
        assertFalse(allow(service(60, 32, "deny", 0), "c1", "subscribe", "a/#"));
    }

    @Test
    @DisplayName("缓存命中不再发请求(action/topic 分别缓存)")
    void cacheHitAvoidsHttpRequest() {
        HttpAclService service = service(60, 32, "deny", 0);
        allow(service, "c1", "publish", "a/b");
        allow(service, "c1", "publish", "a/b");
        allow(service, "c1", "publish", "a/b");
        assertEquals(1, hits.get(), "同 client+action+topic 只应请求一次");

        allow(service, "c1", "publish", "a/c");   // 不同 topic
        allow(service, "c1", "subscribe", "a/b"); // 不同 action
        assertEquals(3, hits.get());
    }

    @Test
    @DisplayName("TTL 过期后重新询问")
    void ttlExpiryRequeries() {
        HttpAclService service = service(0, 32, "deny", 0); // ttl=0 表示不缓存
        allow(service, "c1", "publish", "a/b");
        allow(service, "c1", "publish", "a/b");
        assertEquals(2, hits.get(), "ttl=0 时每次都要问");
    }

    @Test
    @DisplayName("单客户端缓存条目上限: 挤出最旧的(LRU)")
    void perClientLruEviction() {
        HttpAclService service = service(60, 2, "deny", 0);
        allow(service, "c1", "publish", "t1");       // [t1]
        allow(service, "c1", "publish", "t2");       // [t1, t2]
        allow(service, "c1", "publish", "t3");       // 超上限, 挤出 t1 → [t2, t3]
        assertEquals(3, hits.get());
        allow(service, "c1", "publish", "t1");       // t1 已被挤出 → 重新询问, 重入缓存挤出 t2 → [t3, t1]
        assertEquals(4, hits.get());
        allow(service, "c1", "publish", "t2");       // t2 被挤出 → 重新询问 → [t3, t1, t2] 挤出 t3? 不:
        assertEquals(5, hits.get());                 // 上限 2, 重入 t2 挤出 t3 → [t1, t2]
        allow(service, "c1", "publish", "t1");       // t1 仍在缓存(最近访问)
        assertEquals(5, hits.get(), "LRU: 最近访问的条目不应被挤出");
    }

    @Test
    @DisplayName("断连清理: 客户端下线后缓存条目释放")
    void clientOfflineEvictsCache() {
        HttpAclService service = service(60, 32, "deny", 0);
        allow(service, "c1", "publish", "a/b");
        service.onClientOffline("c1");
        allow(service, "c1", "publish", "a/b");
        assertEquals(2, hits.get(), "断连清理后应重新询问");
    }

    @Test
    @DisplayName("result=ignore 按 on-fail 策略: 默认 deny")
    void ignoreFollowsOnFailDeny() {
        body = "{\"result\":\"ignore\"}";
        assertFalse(allow(service(60, 32, "deny", 0), "c1", "publish", "a/b"));
    }

    @Test
    @DisplayName("result=ignore 按 on-fail 策略: 可配 allow")
    void ignoreFollowsOnFailAllow() {
        body = "{\"result\":\"ignore\"}";
        assertTrue(allow(service(60, 32, "allow", 0), "c1", "publish", "a/b"));
    }

    @Test
    @DisplayName("ACL 服务 5xx: on-fail=deny 拒绝且进入熔断冷却")
    void serverErrorDeniesAndCooldowns() {
        status = 500;
        HttpAclService service = service(60, 32, "deny", 60_000);
        long start = System.nanoTime();
        assertFalse(allow(service, "c1", "publish", "a/b"));
        assertEquals(1, hits.get());

        start = System.nanoTime();
        assertFalse(allow(service, "c1", "publish", "x/y"), "冷却期内直接按策略拒绝");
        long elapsed = (System.nanoTime() - start) / 1_000_000;
        assertEquals(1, hits.get(), "冷却期内不得再发请求");
        assertTrue(elapsed < 300, "冷却期内必须短路, 实际 " + elapsed + "ms");
    }

    @Test
    @DisplayName("故障期间的决策不进缓存(服务恢复后重新判定)")
    void failureDecisionsNotCached() {
        status = 500;
        HttpAclService service = service(60, 32, "deny", 0); // 不冷却, 便于观察
        assertFalse(allow(service, "c1", "publish", "a/b"));
        status = 200;
        body = "{\"result\":\"allow\"}";
        assertTrue(allow(service, "c1", "publish", "a/b"), "服务恢复后必须重新判定为 allow");
    }

    @Test
    @DisplayName("enabled=false 时恒为 allow(零外部行为)")
    void disabledAlwaysAllows() {
        HttpAclProperties properties = new HttpAclProperties(false, null, "post",
                500, 500, 60, 32, "deny", 0, Map.of());
        HttpAclService service = new HttpAclService(properties);
        CompletableFuture<Boolean> future =
                service.check("c1", "user", "10.0.0.1", "publish", "a/b");
        assertTrue(future.isDone(), "关闭时必须同步完成");
        assertEquals(0, hits.get());
        try {
            assertTrue(future.get(1, TimeUnit.SECONDS));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @Test
    @DisplayName("enabled=true 但 url 为空: 构造即失败(fail-fast)")
    void blankUrlFailsFast() {
        HttpAclProperties bad = new HttpAclProperties(true, " ", "post",
                500, 500, 60, 32, "deny", 0, Map.of());
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> new HttpAclService(bad));
    }
}
