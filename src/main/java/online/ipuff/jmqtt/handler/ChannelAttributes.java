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
package online.ipuff.jmqtt.handler;

import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.util.AttributeKey;
import online.ipuff.jmqtt.protocol.ConnectOptions;
import online.ipuff.jmqtt.session.SendBuffer;

/**
 * Channel 上的属性键。
 *
 * <p>集中定义而不是各处现写 {@code AttributeKey.valueOf("...")}: 字符串拼错了不会报错,
 * 只会得到一个永远为 null 的属性 —— 这类问题在运行时极难定位。
 */
public final class ChannelAttributes {

    /**
     * 握手成功后写入的 clientId。
     */
    public static final AttributeKey<String> CLIENT_ID = AttributeKey.valueOf("jmqtt.clientId");

    /**
     * 该连接的发送缓冲(在途窗口 + 有界队列)。
     *
     * <p>挂在 Channel 上而不是客户端映射表里: 它的生命周期就是这条 TCP 连接,
     * 连接一断就该回收, 不需要任何额外的清理逻辑。
     */
    public static final AttributeKey<SendBuffer> SEND_BUFFER = AttributeKey.valueOf("jmqtt.sendBuffer");

    /**
     * 协商出的协议版本(v3.1.1 / v5)。
     *
     * <p>我们<b>自己记一份</b>, 而不是复用 Netty 内部的同名属性 ——
     * Netty 把那个 key 定义成包级私有({@code MqttCodecUtil.MQTT_VERSION_KEY}),
     * 应用代码读不到。注意 Netty 的编码器也按它自己的属性决定编包格式,
     * 那份由 {@code MqttDecoder} 在解出 CONNECT 后自动写入, 两者必须一致
     * （不一致就会出现「按 v5 构造报文、按 v3 编码」的坏字节）。见 {@code ReplyFactory.versionOf}。
     */
    public static final AttributeKey<MqttVersion> PROTOCOL_VERSION =
            AttributeKey.valueOf("jmqtt.protocolVersion");

    /**
     * CONNECT 解析出的连接选项(会话保留时长、Receive Maximum、最大报文等)。
     *
     * <p>握手时写入一次, 之后只读。有了它, 后续处理器不必再回头解析 CONNECT 的 v5 属性。
     */
    public static final AttributeKey<ConnectOptions> CONNECT_OPTIONS =
            AttributeKey.valueOf("jmqtt.connectOptions");

    /**
     * 握手成功的时间(epoch millis)。
     *
     * <p>取的是 CONNECT 处理完成的时间, 不是 TCP 建立时间 —— 管理面上「这个客户端连了多久」
     * 指的一定是业务意义上的接入时长。副作用是: 收到 TCP 却始终不发 CONNECT 的连接
     * 不会出现在客户端列表里(它们本来也没有 clientId), 这类连接的规模由节点概要里的
     * {@code rawConnections - connections} 反映。
     */
    public static final AttributeKey<Long> CONNECTED_AT = AttributeKey.valueOf("jmqtt.connectedAt");

    /**
     * 协商后的心跳间隔(秒)。已经过 {@code server-keep-alive} 覆盖, 即真实生效值。
     */
    public static final AttributeKey<Integer> KEEP_ALIVE = AttributeKey.valueOf("jmqtt.keepAlive");

    /**
     * 抑制遗嘱发布。
     *
     * <p><b>只由管理面的驱逐(排水)设置。</b>服务端主动驱逐客户端时, 客户端会在秒级内
     * 重连到其他节点 —— 此时按规范发布遗嘱会在一分钟内产生上万条遗嘱消息, 涌入路由、
     * 集群总线与保留存储, 而设备本身并没有真的下线。这是一个由运维动作自己制造的
     * 故障放大源, 所以给驱逐留一个显式的跳过开关。
     *
     * <p>默认<b>不设置</b>(即按规范发布遗嘱) —— 抑制是运维在特定场景下的选择, 不是默认语义。
     */
    public static final AttributeKey<Boolean> SUPPRESS_WILL =
            AttributeKey.valueOf("jmqtt.suppressWill");

    /**
     * HTTP 鉴权返回 {@code superuser=true} 时置位。
     *
     * <p>EMQX 语义: 超级用户跳过后续全部 ACL 检查。默认<b>不设置</b>(即普通连接,
     * 每次订阅/发布照常走 ACL)。认证链上只有认证服务能授予它, 配置改不出来。
     */
    public static final AttributeKey<Boolean> SUPERUSER = AttributeKey.valueOf("jmqtt.superuser");

    /**
     * CONNECT 认证挂起中(HTTP 鉴权等待外部响应)。
     *
     * <p>置位期间入口忽略该连接上的一切后续报文: 规范要求客户端必须等 CONNACK,
     * 此时到来的任何报文都是异常流量, 而且连接状态尚未初始化, 处理它们必然读到空值。
     * 认证结束(无论放行/拒绝/连接已断)即清除。
     */
    public static final AttributeKey<Boolean> AUTH_PENDING = AttributeKey.valueOf("jmqtt.authPending");

    /**
     * CONNECT 认证通过后写入的用户名(可能为 null)。
     *
     * <p>ACL 请求要带上它 —— 授权服务普遍按用户/角色放行, 只有 clientId 不够用。
     */
    public static final AttributeKey<String> USERNAME = AttributeKey.valueOf("jmqtt.username");

    /**
     * 每连接的 ACL 串行门(懒创建, 见 {@code PublishHandler}/{@code SubscribeHandler})。
     *
     * <p>ACL 判定挂起期间后续报文按序排队, 保住同一连接的报文顺序。
     */
    public static final AttributeKey<online.ipuff.jmqtt.authz.AclGate> ACL_GATE =
            AttributeKey.valueOf("jmqtt.aclGate");

    /**
     * 对端 IP, 供 HTTP 认证/ACL 服务按来源网段判定。
     * 非 InetSocketAddress(如测试通道)时退化为字符串描述, 可能为 null。
     */
    public static String peerhostOf(Channel channel) {
        java.net.SocketAddress remote = channel.remoteAddress();
        if (remote instanceof java.net.InetSocketAddress address && address.getAddress() != null) {
            return address.getAddress().getHostAddress();
        }
        return remote == null ? null : remote.toString();
    }

    private ChannelAttributes() {
    }
}
