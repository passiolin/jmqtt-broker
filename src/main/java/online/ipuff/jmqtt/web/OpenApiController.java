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
package online.ipuff.jmqtt.web;

import online.ipuff.jmqtt.cluster.ClusterBus;
import online.ipuff.jmqtt.cluster.ClusterBusStats;
import online.ipuff.jmqtt.cluster.InternalCommunication;
import online.ipuff.jmqtt.cluster.InternalMessage;
import online.ipuff.jmqtt.cluster.InternalSendServer;
import online.ipuff.jmqtt.config.BrokerProperties;
import online.ipuff.jmqtt.router.MqttTopic;
import online.ipuff.jmqtt.session.BackpressureMetrics;
import online.ipuff.jmqtt.session.QosMetrics;
import online.ipuff.jmqtt.session.ConnectionRegistry;
import online.ipuff.jmqtt.session.ISessionStoreService;
import online.ipuff.jmqtt.store.IInboundQos2Store;
import online.ipuff.jmqtt.store.IPendingMessageStore;
import online.ipuff.jmqtt.store.IRetainMessageStoreService;
import online.ipuff.jmqtt.subscribe.ISubscribeStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 开放 HTTP API。
 *
 * <p><b>{@code /info} 端点直接读内存计数器, 不做任何存储扫描。</b>
 * 用遍历外部存储(如 {@code SCAN})的方式统计在线数, 在十万级连接下是一次昂贵的全量操作,
 * 而这个端点往往被监控系统高频轮询 —— 两件事叠在一起会反噬接入本身。这里所有指标都是
 * 进程内的原子计数, O(1) 且不产生外部调用。
 */
@RestController
@RequestMapping("/open/api/jmqtt")
public class OpenApiController {

    private static final Logger log = LoggerFactory.getLogger(OpenApiController.class);

    private final BrokerProperties properties;
    private final InternalCommunication internalCommunication;
    private final InternalSendServer internalSendServer;
    private final ConnectionRegistry connectionRegistry;
    private final ISessionStoreService sessionStoreService;
    private final ISubscribeStoreService subscribeStoreService;
    private final IRetainMessageStoreService retainMessageStoreService;
    private final ClusterBus clusterBus;
    private final ObjectProvider<ClusterBusStats> clusterBusStats;
    private final IPendingMessageStore pendingMessageStore;
    private final BackpressureMetrics backpressureMetrics;
    private final QosMetrics qosMetrics;
    private final IInboundQos2Store inboundQos2Store;
    private final ObjectProvider<online.ipuff.jmqtt.store.InflightPersistence> inflightPersistence;

    public OpenApiController(BrokerProperties properties,
                             InternalCommunication internalCommunication,
                             InternalSendServer internalSendServer,
                             ConnectionRegistry connectionRegistry,
                             ISessionStoreService sessionStoreService,
                             ISubscribeStoreService subscribeStoreService,
                             IRetainMessageStoreService retainMessageStoreService,
                             ClusterBus clusterBus,
                             ObjectProvider<ClusterBusStats> clusterBusStats,
                             IPendingMessageStore pendingMessageStore,
                             BackpressureMetrics backpressureMetrics,
                             QosMetrics qosMetrics,
                             IInboundQos2Store inboundQos2Store,
                             ObjectProvider<online.ipuff.jmqtt.store.InflightPersistence> inflightPersistence) {
        this.properties = properties;
        this.internalCommunication = internalCommunication;
        this.internalSendServer = internalSendServer;
        this.connectionRegistry = connectionRegistry;
        this.sessionStoreService = sessionStoreService;
        this.subscribeStoreService = subscribeStoreService;
        this.retainMessageStoreService = retainMessageStoreService;
        this.clusterBus = clusterBus;
        this.clusterBusStats = clusterBusStats;
        this.pendingMessageStore = pendingMessageStore;
        this.backpressureMetrics = backpressureMetrics;
        this.qosMetrics = qosMetrics;
        this.inboundQos2Store = inboundQos2Store;
        this.inflightPersistence = inflightPersistence;
    }

    /**
     * 向订阅者投递一条消息(服务端主动下发)。
     *
     * <pre>
     * POST /open/api/jmqtt/send
     * {"topic":"device/123/cmd","qos":1,"retain":false,"message":"hello"}
     * </pre>
     */
    @PostMapping("/send")
    public ResponseEntity<Map<String, Object>> send(@RequestBody SendRequest request) {
        if (request.topic() == null || !MqttTopic.isValidTopicName(request.topic())) {
            return ResponseEntity.badRequest().body(body(false, "非法的主题名: " + request.topic()));
        }
        int qos = request.qos() == null ? 0 : request.qos();
        if (qos < 0 || qos > 2) {
            return ResponseEntity.badRequest().body(body(false, "非法的 QoS: " + qos));
        }

        byte[] payload = request.message() == null
                ? new byte[0]
                : request.message().getBytes(StandardCharsets.UTF_8);

        try {
            // clientId 置空: 服务端下发不针对特定发布者, 无需排除自身
            InternalMessage message = internalCommunication.fromLocal(
                    null, request.topic(), qos, payload, false, request.dup() != null && request.dup());

            int delivered = internalSendServer.sendPublishMessage(message);
            internalCommunication.internalSend(message);

            Map<String, Object> data = new LinkedHashMap<>();
            data.put("topic", request.topic());
            data.put("qos", qos);
            data.put("localDelivered", delivered);
            return ResponseEntity.ok(body(true, null, data));
        } catch (Exception e) {
            log.error("下发消息失败 topic={}", request.topic(), e);
            return ResponseEntity.internalServerError().body(body(false, e.getMessage()));
        }
    }

    /**
     * Broker 运行状态。
     *
     * <p>注意这里全部是内存计数器, 没有全量扫描。
     */
    @GetMapping("/info")
    public ResponseEntity<Map<String, Object>> info() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("brokerId", properties.id());
        data.put("port", properties.port());
        data.put("websocketEnabled", properties.websocketEnabled());
        data.put("websocketPort", properties.websocketPort());
        data.put("clusterEnabled", properties.clusterEnabled());
        data.put("clusterBusEnabled", clusterBus.enabled());
        data.put("uplinkEnabled", clusterBus.uplinkEnabled());

        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("connections", connectionRegistry.size());
        metrics.put("sessions", sessionStoreService.size());
        metrics.put("subscriptions", subscribeStoreService.subscriptionCount());
        metrics.put("topicTreeNodes", subscribeStoreService.topicNodeCount());
        metrics.put("retainMessages", retainMessageStoreService.size());
        metrics.put("writeBufferHighWaterMarkHits", connectionRegistry.highWaterMarkCount());
        metrics.put("offlineQueuePersistent", pendingMessageStore.persistent());
        // 非零就在丢应答给离线客户端的 QoS 1/2 消息 —— QoS 保证的实际破损点
        metrics.put("offlineMessagesDropped", pendingMessageStore.droppedCount());
        // 接收方向 QoS 2 状态被跟踪的客户端数。会话销毁/接管/过期回收都会清掉对应的条目,
        // 因此它应当随客户端下落而回落; 只增不减说明 clearClient 有遗漏(状态泄漏)
        metrics.put("inboundQos2Clients", inboundQos2Store.clientCount());
        // 背压: sendQueueFullDropped 持续增长说明有客户端的确认速度跟不上投递速度
        data.put("backpressure", backpressureMetrics.stats());
        // QoS 分布: deliveredQos12Permille 决定在途消息持久化该用 sync 还是 async
        data.put("qos", qosMetrics.stats());

        online.ipuff.jmqtt.store.InflightPersistence inflight = inflightPersistence.getIfAvailable();
        if (inflight != null) {
            data.put("inflight", inflight.stats());
        }
        data.put("metrics", metrics);

        // 集群总线指标(可选)。dropped 与 outboxSize 是最需要盯的两个:
        // 前者上升说明写总线跟不上, 后者接近容量说明马上要开始丢。
        ClusterBusStats busStats = clusterBusStats.getIfAvailable();
        if (busStats != null) {
            Map<String, Long> stats = busStats.stats();
            if (!stats.isEmpty()) {
                data.put("clusterBus", stats);
            }
        }

        return ResponseEntity.ok(body(true, null, data));
    }

    private static Map<String, Object> body(boolean success, String message) {
        return body(success, message, null);
    }

    private static Map<String, Object> body(boolean success, String message, Object data) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("success", success);
        if (message != null) {
            body.put("message", message);
        }
        if (data != null) {
            body.put("data", data);
        }
        return body;
    }

    /**
     * 下发消息请求体。
     *
     * @param topic   主题名(不得含通配符)
     * @param qos     投递 QoS, 默认 0
     * @param retain  是否作为保留消息(当前仅透传, 不落保留存储)
     * @param dup     是否置 dup 标志
     * @param message 消息正文(UTF-8)
     */
    public record SendRequest(String topic, Integer qos, Boolean retain, Boolean dup, String message) {
    }
}
