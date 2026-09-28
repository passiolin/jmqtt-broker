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
package online.ipuff.jmqtt.store;

import online.ipuff.jmqtt.TestBrokerProperties;
import online.ipuff.jmqtt.TestObjectProviders;
import online.ipuff.jmqtt.config.BrokerProperties;
import online.ipuff.jmqtt.message.DupPublishMessageStore;
import online.ipuff.jmqtt.redis.RedisConnectionManager;
import online.ipuff.jmqtt.session.SessionStore;
import online.ipuff.jmqtt.session.SessionStoreService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 在途镜像写入策略测试。
 *
 * <p>这里不连 Redis —— 被测的是<b>策略</b>: 谁的快照才值得落盘。
 * 与 {@code SessionPersistenceTest} 是同一条策略的两面:
 * 非保留会话(cleanSession=1)的镜像永远不会被读回来, 写它就是白费 Redis;
 * 而且这类写入还有一个真实危害 —— 节点崩溃后客户端改以 cleanSession=0
 * 重连时, 残留镜像会被误当作可恢复状态载入重发, 违反「旧会话应被整体丢弃」。
 */
class InflightPersistenceTest {

    private static final String CLIENT = "device-1";

    private RedisConnectionManager redis;
    private List<String> ops;
    private SessionStoreService sessions;

    @BeforeEach
    void setUp() {
        redis = mock(RedisConnectionManager.class);
        when(redis.available()).thenReturn(true);
        ops = new ArrayList<>();
        // execute 是所有 Redis 访问的必经之路: 记下操作名即「是否真写了」。
        // 不调用传入的 supplier —— commands() 是 mock, 走进去只会 NPE
        when(redis.execute(anyString(), any(), any())).thenAnswer(invocation -> {
            String op = invocation.getArgument(0);
            ops.add(op);
            return "inflight.readExpire".equals(op) ? (Object) "7200" : (Object) 1L;
        });
        sessions = new SessionStoreService();
    }

    private InflightPersistence persistence(String inflightMode) {
        BrokerProperties props = TestBrokerProperties.createWithInflightMode(inflightMode);
        return new InflightPersistence(props, TestObjectProviders.of(redis), Runnable::run, sessions);
    }

    private static DupPublishMessageStore message(int messageId) {
        return new DupPublishMessageStore(CLIENT, "a/b", 1, messageId, new byte[0]);
    }

    @Test
    @DisplayName("cleanSession=1 的非空快照不落盘 —— 它永远不会被读回来")
    void cleanSessionSnapshotIsNotPersisted() {
        sessions.put(new SessionStore("node-1", CLIENT, true, 0));

        persistence("sync").onChanged(CLIENT, List.of(message(1)));

        assertFalse(ops.contains("inflight.persist"),
                "非保留会话不该写镜像, 实际操作=" + ops);
    }

    @Test
    @DisplayName("保留会话(cleanSession=0)的非空快照落盘")
    void retainedSessionSnapshotIsPersisted() {
        sessions.put(new SessionStore("node-1", CLIENT, false, 7200));

        persistence("sync").onChanged(CLIENT, List.of(message(1)));

        assertTrue(ops.contains("inflight.persist"), "保留会话应写镜像, 实际操作=" + ops);
    }

    @Test
    @DisplayName("v5 cleanStart=1 且保留时长>0 的会话同样落盘 —— 判据是保留时长, 不是 cleanStart 位")
    void cleanStartWithExpiryIsPersisted() {
        sessions.put(new SessionStore("node-1", CLIENT, true, 7200));

        persistence("sync").onChanged(CLIENT, List.of(message(1)));

        assertTrue(ops.contains("inflight.persist"), "cleanStart=1 但 SEI>0 应写镜像, 实际操作=" + ops);
    }

    @Test
    @DisplayName("会话不在表里(已销毁/已过期)的非空快照不落盘, 不给幽灵会话写数据")
    void unknownClientSnapshotIsNotPersisted() {
        persistence("sync").onChanged(CLIENT, List.of(message(1)));

        assertFalse(ops.contains("inflight.persist"), "无会话时不应写镜像, 实际操作=" + ops);
    }

    @Test
    @DisplayName("空快照(删除)不受过滤 —— 会话销毁流程里会话可能已被移走, 删除必须照常执行")
    void emptySnapshotDeletionBypassesTheGate() {
        // 不注册任何会话, 模拟销毁路径先删了会话再触发镜像删除
        persistence("sync").onChanged(CLIENT, List.of());

        assertTrue(ops.contains("inflight.persist"), "空快照应照常执行(Lua 脚本对空集合做 DEL), 实际操作=" + ops);
    }

    @Test
    @DisplayName("clear 撤回未刷盘快照并立即删 key —— 删除不滞后一个刷盘周期")
    void clearWithdrawsPendingSnapshot() {
        sessions.put(new SessionStore("node-1", CLIENT, false, 7200));
        InflightPersistence inflight = persistence("async");

        inflight.onChanged(CLIENT, List.of(message(1)));
        assertEquals(1L, inflight.stats().get("inflightPendingClients"), "async 模式应先进入待刷盘");

        inflight.clear(CLIENT);
        assertEquals(0L, inflight.stats().get("inflightPendingClients"), "clear 应撤回待刷盘快照");
        assertTrue(ops.contains("inflight.clear"), "clear 应立即删 key, 实际操作=" + ops);
    }
}
