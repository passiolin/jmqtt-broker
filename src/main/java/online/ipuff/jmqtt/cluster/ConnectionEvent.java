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
package online.ipuff.jmqtt.cluster;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 连接生命周期事件, 发布到 Kafka 的
 * {@code jmqtt.broker.kafka.connection-event-topic} 供后台系统消费。
 *
 * <p>JSON 契约(字段与既有后台消费的事件格式对齐):
 * <pre>
 * {"action":"connected","clientid":"...","username":"...","proto_ver":4,
 *  "proto_name":"MQTT","peername":"10.0.0.7:47414","node":"node-1",
 *  "connected_at":1770000000000}
 *
 * {"action":"disconnected","clientid":"...","username":"...","proto_ver":5,
 *  "proto_name":"MQTT","peername":"10.0.0.7:51166","node":"node-1",
 *  "reason":"tcp_closed","disconnected_at":1770000001000}
 * </pre>
 *
 * <p>record 的 Kafka key 是 clientid —— 同一客户端的事件严格有序,
 * 后台按 key 即可还原设备的完整在线状态机。
 *
 * @param action          {@link #ACTION_CONNECTED} 或 {@link #ACTION_DISCONNECTED}
 * @param clientid        客户端标识
 * @param username        认证用户名, 可能为 null(序列化时省略)
 * @param protoVersion    协议版本号(4 = v3.1.1, 5 = v5.0)
 * @param protoName       协议名, 恒为 "MQTT"
 * @param peername        对端地址 ip:port
 * @param node            产生事件的接入节点(brokerId)
 * @param reason          下线原因, 仅 disconnected 事件: {@code closed}(客户端主动
 *                        DISCONNECT)或 {@code tcp_closed}(连接断开/心跳超时/被接管)
 * @param connectedAt     上线时间(epoch millis), 仅 connected 事件
 * @param disconnectedAt  下线时间(epoch millis), 仅 disconnected 事件
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ConnectionEvent(
        String action,
        String clientid,
        String username,
        @JsonProperty("proto_ver") int protoVersion,
        @JsonProperty("proto_name") String protoName,
        String peername,
        String node,
        String reason,
        @JsonProperty("connected_at") Long connectedAt,
        @JsonProperty("disconnected_at") Long disconnectedAt
) {

    public static final String ACTION_CONNECTED = "connected";

    public static final String ACTION_DISCONNECTED = "disconnected";

    /** 下线原因: 客户端主动发送了 DISCONNECT 报文 */
    public static final String REASON_CLOSED = "closed";

    /** 下线原因: 连接断开 / 心跳超时 / 被接管等非优雅离线 */
    public static final String REASON_TCP_CLOSED = "tcp_closed";

    public static ConnectionEvent connected(String clientid, String username,
                                            int protoVersion, String peername, String node) {
        return new ConnectionEvent(ACTION_CONNECTED, clientid, username,
                protoVersion, "MQTT", peername, node, null, System.currentTimeMillis(), null);
    }

    public static ConnectionEvent disconnected(String clientid, String username,
                                               int protoVersion, String peername, String node,
                                               String reason) {
        return new ConnectionEvent(ACTION_DISCONNECTED, clientid, username,
                protoVersion, "MQTT", peername, node, reason, null, System.currentTimeMillis());
    }
}
