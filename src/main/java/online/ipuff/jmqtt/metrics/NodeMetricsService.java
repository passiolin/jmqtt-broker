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
package online.ipuff.jmqtt.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import online.ipuff.jmqtt.cluster.ClusterBusStats;
import online.ipuff.jmqtt.config.BrokerProperties;
import online.ipuff.jmqtt.session.BackpressureMetrics;
import online.ipuff.jmqtt.session.ConnectionRegistry;
import online.ipuff.jmqtt.session.ISessionStoreService;
import online.ipuff.jmqtt.session.QosMetrics;
import online.ipuff.jmqtt.subscribe.ISubscribeStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * 节点运行时指标 —— <b>只维护最高效的当前状态</b>: 一组 AtomicLong 累计计数与内存 size 仪表。
 *
 * <h2>职责划分: broker 算当前值, 控制台定时采集并存历史</h2>
 * <ul>
 *   <li><b>broker</b>: 只在事件发生时自增计数(连接生命周期在此; QoS 与背压计数在
 *       {@link QosMetrics}/{@link BackpressureMetrics}), 不做任何采样、不留历史序列 ——
 *       历史由控制台的采集器定期来取并写入 Redis。管理命令(METRICS)到达时现场读一次
 *       内存即返回, 单次回复只有几十个数字。</li>
 *   <li><b>外部监控</b>: 同一份计数以 FunctionCounter/Gauge 绑进 Micrometer,
 *       由 actuator 的 /actuator/prometheus 抓取。绑定读的是同一批 AtomicLong,
 *       抓取时才取值 —— <b>消息热路径上没有任何 Micrometer 开销</b>。</li>
 * </ul>
 *
 * <h2>性能边界</h2>
 * 计数全部是本地结构的读与自增(会话/订阅/连接数不碰 Redis);
 * 序列化只发生在命令到达时, 体积与在线时长无关。
 */
@Component
public class NodeMetricsService {

    private static final Logger log = LoggerFactory.getLogger(NodeMetricsService.class);

    private final QosMetrics qosMetrics;
    private final BackpressureMetrics backpressureMetrics;
    private final ConnectionRegistry connectionRegistry;
    private final ISessionStoreService sessionStoreService;
    private final ISubscribeStoreService subscribeStoreService;
    private final ObjectProvider<ClusterBusStats> busStats;
    private final ObjectProvider<MeterRegistry> meterRegistry;
    private final String nodeId;

    private final ObjectMapper objectMapper = new ObjectMapper();

    // ---- 连接生命周期计数(采样与 Micrometer 共同的数据源) ----
    private final AtomicLong connectionsOpened = new AtomicLong();
    private final AtomicLong connectionsClosedGraceful = new AtomicLong();
    private final AtomicLong connectionsClosedAbnormal = new AtomicLong();
    private final AtomicLong connectionsRejectedAuth = new AtomicLong();
    private final AtomicLong connectionsRejectedClientId = new AtomicLong();
    private final AtomicLong connectionTakeoverLocal = new AtomicLong();
    private final AtomicLong connectionTakeoverRemote = new AtomicLong();

    public NodeMetricsService(QosMetrics qosMetrics,
                              BackpressureMetrics backpressureMetrics,
                              ConnectionRegistry connectionRegistry,
                              ISessionStoreService sessionStoreService,
                              ISubscribeStoreService subscribeStoreService,
                              ObjectProvider<ClusterBusStats> busStats,
                              ObjectProvider<MeterRegistry> meterRegistry,
                              BrokerProperties brokerProperties) {
        this.qosMetrics = qosMetrics;
        this.backpressureMetrics = backpressureMetrics;
        this.connectionRegistry = connectionRegistry;
        this.sessionStoreService = sessionStoreService;
        this.subscribeStoreService = subscribeStoreService;
        this.busStats = busStats;
        this.meterRegistry = meterRegistry;
        this.nodeId = brokerProperties.id();
    }

    // ------------------------------------------------------------------
    // 埋点入口(连接生命周期; QoS/背压计数由各自的组件直接埋)
    // ------------------------------------------------------------------

    /** CONNACK 成功并完成注册 */
    public void connectionOpened() {
        connectionsOpened.incrementAndGet();
    }

    /** 连接关闭。优雅(DISCONNECT 主动断开)与异常(超时/TCP 断开)分开计 */
    public void connectionClosed(boolean graceful) {
        (graceful ? connectionsClosedGraceful : connectionsClosedAbnormal).incrementAndGet();
    }

    /** CONNECT 被拒绝。reason: auth / clientId */
    public void connectionRejected(String reason) {
        ("auth".equals(reason) ? connectionsRejectedAuth : connectionsRejectedClientId).incrementAndGet();
    }

    /** 同一 clientId 的新连接顶掉本节点旧连接 */
    public void takeoverLocal() {
        connectionTakeoverLocal.incrementAndGet();
    }

    /** 收到其他节点的接管通知, 释放本节点连接 */
    public void takeoverRemote() {
        connectionTakeoverRemote.incrementAndGet();
    }

    // ------------------------------------------------------------------
    // 当前状态(管理命令 METRICS)
    // ------------------------------------------------------------------

    /**
     * 当前全量累计值。键空间对所有消费方(管理命令/Micrometer)一致。
     * 本地结构的 size: 内存读, 不碰 Redis。
     */
    public Map<String, Long> currentValues() {
        Map<String, Long> values = new LinkedHashMap<>();
        qosMetrics.stats().forEach((k, v) -> values.put("qos." + k, v));
        backpressureMetrics.stats().forEach((k, v) -> values.put("backpressure." + k, v));
        values.put("connections.opened", connectionsOpened.get());
        values.put("connections.closedGraceful", connectionsClosedGraceful.get());
        values.put("connections.closedAbnormal", connectionsClosedAbnormal.get());
        values.put("connections.rejectedAuth", connectionsRejectedAuth.get());
        values.put("connections.rejectedClientId", connectionsRejectedClientId.get());
        values.put("connections.takeoverLocal", connectionTakeoverLocal.get());
        values.put("connections.takeoverRemote", connectionTakeoverRemote.get());
        values.put("connections.active", (long) connectionRegistry.size());
        values.put("sessions.active", (long) sessionStoreService.size());
        values.put("subscriptions.active", (long) subscribeStoreService.subscriptionCount());
        ClusterBusStats bus = busStats.getIfAvailable();
        if (bus != null) {
            bus.stats().forEach((k, v) -> values.put("bus." + k, v));
        }
        return values;
    }

    /**
     * 当前快照(管理命令 METRICS 的回复载荷): 只有累计值, 没有历史 ——
     * 历史序列由控制台的采集器负责。
     */
    public String snapshotJson() {
        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("node", nodeId);
        snapshot.put("generatedAt", System.currentTimeMillis());
        snapshot.put("values", currentValues());
        try {
            return objectMapper.writeValueAsString(snapshot);
        } catch (Exception e) {
            // 序列化失败(理论上不可能)按空快照处理, 不让观测请求打断命令执行
            log.warn("指标快照序列化失败", e);
            return "{}";
        }
    }

    // ------------------------------------------------------------------
    // Prometheus(actuator /actuator/prometheus)
    // ------------------------------------------------------------------

    /**
     * 把计数绑进 Micrometer。全部用 FunctionCounter/Gauge: 注册一次, 抓取时才读值,
     * 与消息热路径完全解耦。标签统一带 node, 多节点抓到同一个 Prometheus 时可直接聚合。
     */
    @EventListener(ApplicationReadyEvent.class)
    public void bindToMicrometer() {
        MeterRegistry registry = meterRegistry.getIfAvailable();
        if (registry == null) {
            return;
        }
        try {
            Gauge.builder("jmqtt.connections.active", connectionRegistry,
                            r -> r.size()).tag("node", nodeId)
                    .description("Current MQTT connections").register(registry);
            Gauge.builder("jmqtt.sessions.active", sessionStoreService,
                            ISessionStoreService::size).tag("node", nodeId)
                    .description("Current sessions").register(registry);
            Gauge.builder("jmqtt.subscriptions.active", subscribeStoreService,
                            ISubscribeStoreService::subscriptionCount).tag("node", nodeId)
                    .description("Current subscriptions").register(registry);

            for (int qos = 0; qos <= 2; qos++) {
                int q = qos;
                bindCounter(registry, "jmqtt.messages.published", "qos", String.valueOf(q),
                        () -> qosMetrics.stats().get("publishedQos" + q));
                bindCounter(registry, "jmqtt.messages.delivered", "qos", String.valueOf(q),
                        () -> qosMetrics.stats().get("deliveredQos" + q));
            }
            bindCounter(registry, "jmqtt.messages.dropped", "reason", "qos0_not_writable",
                    () -> backpressureMetrics.stats().get("qos0NotWritableDropped"));
            bindCounter(registry, "jmqtt.messages.dropped", "reason", "send_queue_full",
                    () -> backpressureMetrics.stats().get("sendQueueFullDropped"));
            bindCounter(registry, "jmqtt.send.enqueued", "none", "none",
                    () -> backpressureMetrics.stats().get("sendQueueEnqueued"));

            bindCounter(registry, "jmqtt.connections.opened", "none", "none",
                    connectionsOpened::get);
            bindCounter(registry, "jmqtt.connections.closed", "reason", "graceful",
                    connectionsClosedGraceful::get);
            bindCounter(registry, "jmqtt.connections.closed", "reason", "abnormal",
                    connectionsClosedAbnormal::get);
            bindCounter(registry, "jmqtt.connections.rejected", "reason", "auth",
                    connectionsRejectedAuth::get);
            bindCounter(registry, "jmqtt.connections.rejected", "reason", "client_id",
                    connectionsRejectedClientId::get);
            bindCounter(registry, "jmqtt.connections.takeover", "type", "local",
                    connectionTakeoverLocal::get);
            bindCounter(registry, "jmqtt.connections.takeover", "type", "remote",
                    connectionTakeoverRemote::get);

            ClusterBusStats bus = busStats.getIfAvailable();
            if (bus != null) {
                // 总线指标键集合随实现固定(KafkaClusterBus 输出 dropped/outboxSize 等),
                // 注册时读一次键名, 值在抓取时实时读
                for (String key : List.copyOf(bus.stats().keySet())) {
                    bindCounter(registry, "jmqtt.bus." + key.replace('_', '.'), "none", "none",
                            () -> bus.stats().get(key));
                }
            }
        } catch (Exception e) {
            // 指标绑定失败不能拖垮 broker 启动 —— 观测永远低于业务
            log.warn("Micrometer 指标绑定失败, /actuator/prometheus 将缺少 jmqtt 指标", e);
        }
    }

    private void bindCounter(MeterRegistry registry, String name, String tagKey, String tagValue,
                             Supplier<Long> value) {
        FunctionCounter.builder(name, this, v -> {
                    Long n = value.get();
                    return n == null ? 0L : n;
                }).tag("node", nodeId).tag(tagKey, tagValue)
                .description("jmqtt cumulative counter").register(registry);
    }
}
