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
package online.ipuff.jmqtt.protocol;

import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.CharsetUtil;
import online.ipuff.jmqtt.admin.AdminStatePublisher;
import online.ipuff.jmqtt.auth.IAuthService;
import online.ipuff.jmqtt.cluster.ClusterBus;
import online.ipuff.jmqtt.cluster.InternalSendServer;
import online.ipuff.jmqtt.config.BrokerProperties;
import online.ipuff.jmqtt.handler.ChannelAttributes;
import online.ipuff.jmqtt.message.DupPubRelMessageStore;
import online.ipuff.jmqtt.message.DupPublishMessageStore;
import online.ipuff.jmqtt.message.SubscribeStore;
import online.ipuff.jmqtt.message.WillMessage;
import online.ipuff.jmqtt.session.BackpressureMetrics;
import online.ipuff.jmqtt.session.ConnectionRegistry;
import online.ipuff.jmqtt.session.ISessionStoreService;
import online.ipuff.jmqtt.session.SendBuffer;
import online.ipuff.jmqtt.session.SessionStore;
import online.ipuff.jmqtt.session.persistence.PersistedSession;
import online.ipuff.jmqtt.session.persistence.SessionPersistence;
import online.ipuff.jmqtt.store.IDupPubRelMessageStoreService;
import online.ipuff.jmqtt.store.IDupPublishMessageStoreService;
import online.ipuff.jmqtt.store.IInboundQos2Store;
import online.ipuff.jmqtt.store.InflightPersistence;
import online.ipuff.jmqtt.subscribe.ISubscribeStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

/**
 * CONNECT 处理。
 *
 * <h2>sessionPresent 必须按「服务端是否真的持有会话」计算</h2>
 * 一个很自然但错误的写法是:
 * <pre>
 *   boolean sessionPresent = !msg.variableHeader().isCleanSession();
 * </pre>
 * 即<b>纯按客户端传来的 cleanSession 标志位</b>决定, 完全不看服务端是否真的持有会话。
 * 后果: 客户端以 {@code cleanSession=0} 连到一个没有该会话的 broker, 会收到
 * {@code sessionPresent=true} —— 客户端据此认为订阅仍然有效而不重新订阅,
 * 但服务端一条订阅都没有, 于是<b>静默丢消息, 不报错也不断连</b>。
 *
 * <p>这里改为按「服务端是否真的恢复了会话」计算, 并且只在订阅确实保留时才回 true。
 *
 * <h2>v5 带来的三处结构性变化</h2>
 * <ol>
 *   <li><b>版本要记在 Channel 上。</b>回包形态由版本决定, 而回包发生在后续十几处,
 *       所以握手时把版本与解析好的 {@link ConnectOptions} 一起写进 Channel 属性
 *       （见 {@link ChannelAttributes#PROTOCOL_VERSION}）。</li>
 *   <li><b>「保留会话」的判据换了。</b>v3.1.1 是 {@code !cleanSession}, v5 是
 *       Session Expiry Interval &gt; 0 —— 两者并不等价: v5 允许
 *       {@code cleanStart=1} 且 SEI &gt; 0（从零开始建立、建立后保留）。
 *       统一用 {@link SessionStore#isPersistent()}。</li>
 *   <li><b>在途窗口由客户端参与决定。</b>v5 的 Receive Maximum 声明了客户端能同时容纳
 *       多少条未确认的 QoS 1/2, 服务端必须遵守; v3.1.1 没有这个概念, 只受服务端自己的
 *       {@code max-inflight} 约束。取两者最小值。</li>
 * </ol>
 *
 * <h2>v5 能力声明为什么放在 CONNACK 里</h2>
 * v5 的 CONNACK 可以声明服务端支持哪些特性（Maximum QoS / Retain Available /
 * Wildcard Subscription Available / Shared Subscription Available /
 * Subscription Identifiers Available）。<b>用声明代替猜测</b>是这个版本最重要的改进 ——
 * 与其让客户端试了才知道, 不如一次说清。本实现声明:
 * 通配符订阅支持、保留消息支持、QoS 2 支持;
 * 共享订阅与订阅标识符不支持（前者的正确实现牵涉集群路由, 后者的意义在于区分同一条
 * 消息的多次投递, 都需要单独设计）。
 */
@Component
public class ConnectHandler {

    private static final Logger log = LoggerFactory.getLogger(ConnectHandler.class);

    /** 心跳宽限期系数: 服务端等待 keepAlive * 1.5 后判定离线 */
    private static final float KEEP_ALIVE_BACKOFF = 1.5f;

    private final BrokerProperties properties;
    private final IAuthService authService;
    private final ISessionStoreService sessionStoreService;
    private final ISubscribeStoreService subscribeStoreService;
    private final IDupPublishMessageStoreService dupPublishMessageStoreService;
    private final IDupPubRelMessageStoreService dupPubRelMessageStoreService;
    private final ConnectionRegistry connectionRegistry;
    private final SessionPersistence sessionPersistence;
    private final ClusterBus clusterBus;
    private final InternalSendServer internalSendServer;
    private final BackpressureMetrics backpressureMetrics;
    private final InflightPersistence inflightPersistence;
    private final AdminStatePublisher adminStatePublisher;
    private final IInboundQos2Store inboundQos2Store;

    public ConnectHandler(BrokerProperties properties,
                          IAuthService authService,
                          ISessionStoreService sessionStoreService,
                          ISubscribeStoreService subscribeStoreService,
                          IDupPublishMessageStoreService dupPublishMessageStoreService,
                          IDupPubRelMessageStoreService dupPubRelMessageStoreService,
                          ConnectionRegistry connectionRegistry,
                          SessionPersistence sessionPersistence,
                          ClusterBus clusterBus,
                          InternalSendServer internalSendServer,
                          BackpressureMetrics backpressureMetrics,
                          InflightPersistence inflightPersistence,
                          AdminStatePublisher adminStatePublisher,
                          IInboundQos2Store inboundQos2Store) {
        this.properties = properties;
        this.authService = authService;
        this.sessionStoreService = sessionStoreService;
        this.subscribeStoreService = subscribeStoreService;
        this.dupPublishMessageStoreService = dupPublishMessageStoreService;
        this.dupPubRelMessageStoreService = dupPubRelMessageStoreService;
        this.connectionRegistry = connectionRegistry;
        this.sessionPersistence = sessionPersistence;
        this.clusterBus = clusterBus;
        this.internalSendServer = internalSendServer;
        this.backpressureMetrics = backpressureMetrics;
        this.inflightPersistence = inflightPersistence;
        this.adminStatePublisher = adminStatePublisher;
        this.inboundQos2Store = inboundQos2Store;
    }

    public void processConnect(Channel channel, MqttConnectMessage msg) {
        // 协议级别只有 4(v3.1.1) 与 5(v5.0) 两种需要区分; 其余值 Netty 解码器
        // 已在生成报文阶段就抛出 MqttUnacceptableProtocolVersionException 拒绝了,
        // 走不到这里, 所以不必再判一次。
        MqttVersion version = msg.variableHeader().version() == ConnectOptions.VERSION_5
                ? MqttVersion.MQTT_5 : MqttVersion.MQTT_3_1_1;
        // 必须尽早写入: 之后所有回包(包括拒绝)都要按这个版本构造
        channel.attr(ChannelAttributes.PROTOCOL_VERSION).set(version);

        ConnectOptions options = ConnectOptions.parse(msg, properties);
        channel.attr(ChannelAttributes.CONNECT_OPTIONS).set(options);

        String clientId = msg.payload().clientIdentifier();
        boolean assignedClientId = false;

        // 1) clientId
        if (clientId == null || clientId.isEmpty()) {
            // v5 允许服务端分配 clientId（并且必须回 Assigned Client Identifier）;
            // v3.1.1 没有这个机制, 只能拒绝
            if (options.isV5()) {
                clientId = generateClientId();
                assignedClientId = true;
                log.debug("v5 客户端未提供 clientId, 已分配: {}", clientId);
            } else {
                log.debug("CONNECT 被拒绝: clientId 为空");
                rejectAndClose(channel, version, ReplyFactory.ConnAckReason.CLIENT_IDENTIFIER_NOT_VALID);
                return;
            }
        }

        // 2) 认证
        if (!authenticate(channel, msg, version)) {
            return;
        }

        // 3) 会话处理 —— 决定 sessionPresent
        SessionStore existing = sessionStoreService.get(clientId);
        boolean sessionPresent = false;

        if (options.cleanStart()) {
            // 规范要求请求丢弃既有会话时, 服务端丢弃既有会话及其全部状态
            clearSessionState(clientId);
            existing = null;
        } else if (existing != null) {
            // 本进程内仍持有该会话, 订阅也还在本地主题树里
            sessionPresent = true;
        } else {
            // 本进程没有 —— 尝试从持久层恢复(该会话可能此前归属其他节点)
            PersistedSession persisted = sessionPersistence.restore(clientId);
            if (persisted != null) {
                existing = new SessionStore(properties.id(), clientId, false,
                        persisted.expireSeconds() > 0 ? persisted.expireSeconds() : properties.sessionExpirySeconds(),
                        null, persisted.createdAt());
                restoreSubscriptions(persisted);
                sessionPresent = true;
            } else {
                // 持久层也没有: 清理本地可能残留的脏数据
                clearDanglingState(clientId);
            }
        }

        // 4) 遗嘱(含 v5 的 Will Delay Interval, 先解析保存, 投递时机见 WillMessage)
        WillMessage willMessage = extractWillMessage(msg, options);

        // 5) 落库会话
        SessionStore session = (existing != null && !options.cleanStart())
                ? existing
                : new SessionStore(properties.id(), clientId, options.cleanStart(),
                        options.sessionExpirySeconds());
        session.setExpireSeconds(options.sessionExpirySeconds());
        session.setWillMessage(willMessage);
        session.touch();
        sessionStoreService.put(session);

        if (session.isPersistent()) {
            // 顺序很重要: 必须<b>先</b>取归属再写会话。
            // acquireOwnership 会原子地读出旧 owner 并写入本节点;
            // 若先 saveSession, node 字段已是本节点, 就永远读不出旧 owner 了。
            String previousOwner = sessionPersistence.acquireOwnership(
                    clientId, properties.id(), session.getExpireSeconds());
            if (previousOwner != null && !previousOwner.equals(properties.id())) {
                announceTakeover(clientId, previousOwner);
            }
            sessionPersistence.saveSession(session);
        }

        // 6) 心跳: server-keep-alive 覆盖客户端协商值(仅当配置大于 0)
        int keepAlive = properties.serverKeepAlive() > 0
                ? properties.serverKeepAlive()
                : msg.variableHeader().keepAliveTimeSeconds();
        configureKeepAlive(channel, keepAlive);

        // 7) 注册连接, 并踢掉同一 clientId 的旧连接(连接接管)
        channel.attr(ChannelAttributes.CLIENT_ID).set(clientId);
        // 管理面要展示「接入时间 / 心跳 / 协议版本」, 前两项在这里落一次即可 ——
        // 之后它们在连接生命周期内不再变化, 读取方无需回查会话或重解析 CONNECT
        channel.attr(ChannelAttributes.CONNECTED_AT).set(System.currentTimeMillis());
        channel.attr(ChannelAttributes.KEEP_ALIVE).set(keepAlive);
        // 发送缓冲挂在 Channel 上: 它的生命周期就是这条连接, 随连接自动回收。
        // 必须在任何投递发生之前装配好, 否则投递会走「未装配缓冲」的退化路径。
        // 窗口取服务端与客户端声明的较小值 —— v5 的 Receive Maximum 是客户端侧流控
        int window = Math.max(1, Math.min(properties.maxInflight(), options.receiveMaximum()));
        channel.attr(ChannelAttributes.SEND_BUFFER).set(new SendBuffer(
                window, properties.maxMqueueLen(), backpressureMetrics));
        Channel previous = connectionRegistry.register(clientId, channel);
        if (previous != null && previous != channel) {
            log.info("clientId={} 发生连接接管, 关闭旧连接 {}", clientId, previous.id().asShortText());
            previous.close();
        }

        // 8) CONNACK
        channel.writeAndFlush(ReplyFactory.connAck(version,
                MqttConnectReturnCode.CONNECTION_ACCEPTED, sessionPresent,
                buildConnAckProperties(session, options, assignedClientId ? clientId : null, keepAlive)));
        log.debug("CONNECT 成功 clientId={} version={} cleanStart={} sessionPresent={} 会话保留={}s 窗口={}",
                clientId, version, options.cleanStart(), sessionPresent,
                session.getExpireSeconds(), window);

        // 管理面可见性: 只往有界队列里塞一条, 不做任何 Redis 调用。
        // 放在这里而不是更早, 是因为它要读到连接上的全部属性(版本/心跳/时间/发送缓冲)
        // 以及已恢复的订阅 —— 早于恢复订阅就会上报成「零订阅」。
        adminStatePublisher.clientOnline(clientId, channel);

        // 9) 需要保留的会话才做恢复投递: 在途重发 + 离线积压
        if (session.isPersistent()) {
            loadInflightFromMirror(clientId);
            resendInflightMessages(channel, clientId);

            // 10) 投递离线积压 —— 客户端离线期间本应收到但没能送达的 QoS 1/2 消息。
            //     放在重发未确认消息之后: 先补齐已发出的, 再补未发出的, 顺序更贴近真实时序。
            internalSendServer.deliverPendingMessages(clientId, channel);
        }
    }

    /**
     * CONNACK 的 v5 属性。v3.1.1 传 null —— {@link ReplyFactory#connAck} 会用两参构造。
     */
    private MqttProperties buildConnAckProperties(SessionStore session, ConnectOptions options,
                                                  String assignedClientId, int keepAlive) {
        if (!options.isV5()) {
            return null;
        }
        MqttProperties props = new MqttProperties();
        addInt(props, MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL, session.getExpireSeconds());
        addInt(props, MqttProperties.MqttPropertyType.RECEIVE_MAXIMUM, properties.maxInflight());
        addInt(props, MqttProperties.MqttPropertyType.MAXIMUM_PACKET_SIZE, properties.maxPayloadSize());
        // 我们不接受入站 topic alias —— 明确声明 0, 而不是默不作声地忽略它
        addInt(props, MqttProperties.MqttPropertyType.TOPIC_ALIAS_MAXIMUM, properties.topicAliasMaximum());
        // 能力声明: 用声明代替让客户端试错
        addInt(props, MqttProperties.MqttPropertyType.MAXIMUM_QOS, 2);
        addInt(props, MqttProperties.MqttPropertyType.RETAIN_AVAILABLE, 1);
        addInt(props, MqttProperties.MqttPropertyType.WILDCARD_SUBSCRIPTION_AVAILABLE, 1);
        addInt(props, MqttProperties.MqttPropertyType.SHARED_SUBSCRIPTION_AVAILABLE, 0);
        addInt(props, MqttProperties.MqttPropertyType.SUBSCRIPTION_IDENTIFIER_AVAILABLE, 0);
        if (properties.serverKeepAlive() > 0) {
            addInt(props, MqttProperties.MqttPropertyType.SERVER_KEEP_ALIVE, keepAlive);
        }
        if (assignedClientId != null) {
            props.add(new MqttProperties.StringProperty(
                    MqttProperties.MqttPropertyType.ASSIGNED_CLIENT_IDENTIFIER.value(), assignedClientId));
        }
        return props;
    }

    private static void addInt(MqttProperties props, MqttProperties.MqttPropertyType type, long value) {
        props.add(new MqttProperties.IntegerProperty(type.value(), (int) value));
    }

    /**
     * 服务端分配 clientId。带 broker 前缀便于在日志里定位是谁分配的。
     */
    private String generateClientId() {
        return properties.id() + "-" + UUID.randomUUID().toString().substring(0, 12);
    }

    /**
     * 把在途消息的持久镜像加载回内存。
     *
     * <p>这一步让「节点崩溃时已发出但未确认的 QoS 1/2 消息」在客户端重连后仍能重发 ——
     * 否则这些消息只存在于崩溃节点的内存里, 随进程一起消失。
     */
    private void loadInflightFromMirror(String clientId) {
        List<DupPublishMessageStore> inflight = inflightPersistence.load(clientId);
        if (inflight.isEmpty()) {
            return;
        }
        for (DupPublishMessageStore message : inflight) {
            dupPublishMessageStoreService.put(message);
        }
    }

    /**
     * 通知旧节点释放该 clientId 的连接。
     *
     * <p>MQTT 规范要求: 同一 clientId 已有连接时, 服务端必须断开既有连接
     * （v3.1.1 [MQTT-3.1.4-2] / v5 [MQTT-3.1.4-3]）。
     * 单节点内这件事由 {@code ConnectionRegistry.register} 顺手完成; 跨节点则必须显式通知 ——
     * 否则旧节点上的连接会一直存活到它自己的心跳超时(可能几分钟)。
     * 这期间同一个 clientId 在两个节点上同时在线, 表现为共享订阅重复投递、
     * 下行指令被处理两次等难以复现的问题。
     */
    private void announceTakeover(String clientId, String previousOwner) {
        if (!clusterBus.enabled()) {
            log.error("clientId={} 的会话归属在节点 [{}], 但集群总线未启用, 无法通知其释放旧连接。"
                            + "旧连接会存活到自身心跳超时, 期间该客户端实际处于双连状态。"
                            + "请同时开启 cluster-enabled 与 jmqtt.broker.kafka.enabled。",
                    clientId, previousOwner);
            return;
        }
        clusterBus.publishTakeover(clientId, previousOwner);
        log.info("已通知节点 [{}] 释放 clientId={} 的旧连接(跨节点接管)", previousOwner, clientId);
    }

    /**
     * 把持久层恢复出来的订阅写回本地主题树。
     *
     * <p>刻意直接调 {@code subscribeStoreService}, 不经过 {@code SessionPersistence} ——
     * 这些订阅本来就在持久层里, 不需要再回写一遍。
     */
    private void restoreSubscriptions(PersistedSession persisted) {
        for (SubscribeStore subscription : persisted.subscriptions()) {
            subscribeStoreService.put(new SubscribeStore(
                    persisted.clientId(), subscription.topicFilter(), subscription.qos()));
        }
        log.info("已恢复会话订阅: clientId={} 订阅数={}",
                persisted.clientId(), persisted.subscriptions().size());
    }

    private boolean authenticate(Channel channel, MqttConnectMessage msg, MqttVersion version) {
        if (!properties.authEnabled()) {
            return true;
        }
        String username = msg.payload().userName();
        String password = msg.payload().passwordInBytes() == null
                ? null
                : new String(msg.payload().passwordInBytes(), CharsetUtil.UTF_8);

        if (!authService.checkValid(username, password)) {
            log.debug("CONNECT 被拒绝: 认证失败 username={}", username);
            rejectAndClose(channel, version, ReplyFactory.ConnAckReason.BAD_CREDENTIALS);
            return false;
        }
        return true;
    }

    /**
     * 用对应版本的拒绝码回 CONNACK 并关闭连接。
     */
    private void rejectAndClose(Channel channel, MqttVersion version, ReplyFactory.ConnAckReason reason) {
        channel.writeAndFlush(ReplyFactory.connAck(version,
                ReplyFactory.connAckCode(version, reason), false, null))
                .addListener(f -> channel.close());
    }

    /**
     * 清除会话及其全部附属状态, 包含持久层。
     */
    private void clearSessionState(String clientId) {
        sessionStoreService.remove(clientId);
        subscribeStoreService.removeForClient(clientId);
        dupPublishMessageStoreService.removeByClient(clientId);
        dupPubRelMessageStoreService.removeByClient(clientId);
        // 会话重建时接收方向的 QoS 2 状态同样作废: 客户端拿到的是 sessionPresent=false,
        // 它不会(也不该)继续上一次未完成的 PUBREL 流程
        inboundQos2Store.clearClient(clientId);
        sessionPersistence.sessionDestroyed(clientId);
    }

    /**
     * 只清理本地残留。
     *
     * <p><b>刻意不碰持久层。</b>走到这里只说明「本进程内存里没有该会话」,
     * 而这件事有两个完全不同的成因: 会话确实不存在, 或者持久层这一次没读到。
     * 后者会把「Redis 抖动」翻译成「删掉别的节点上那个会话的在途镜像」——
     * 于是客户端重连后既没有会话也没有待重发的消息, QoS 1/2 静默破损。
     * 所以这里只清内存: 会话、订阅、在途镜像在持久层的部分都不归它管,
     * 真该销毁时由 {@link #clearSessionState} 处理。
     */
    private void clearDanglingState(String clientId) {
        subscribeStoreService.removeForClient(clientId);
        dupPublishMessageStoreService.detach(clientId);
        dupPubRelMessageStoreService.removeByClient(clientId);
        // 接收方向的 QoS 2 状态只存在于本进程, 属于「本地残留」的范畴。
        // 留着它会让一个早已作废的标识符继续挡住后续消息
        inboundQos2Store.clearClient(clientId);
    }

    private void configureKeepAlive(Channel channel, int keepAliveSeconds) {
        if (keepAliveSeconds <= 0) {
            // keepAlive=0 表示不使用心跳, 去掉空闲检测
            if (channel.pipeline().get("idle") != null) {
                channel.pipeline().remove("idle");
            }
            return;
        }
        // Math.round(float) 返回 int, 正好匹配 IdleStateHandler(int,int,int) 重载
        int idleSeconds = Math.round(keepAliveSeconds * KEEP_ALIVE_BACKOFF);
        if (channel.pipeline().get("idle") != null) {
            channel.pipeline().replace("idle", "idle", new IdleStateHandler(0, 0, idleSeconds));
        } else {
            channel.pipeline().addFirst("idle", new IdleStateHandler(0, 0, idleSeconds));
        }
    }

    /**
     * 构造遗嘱消息。v5 的 will properties 单独存放, 这里只取 Will Delay Interval
     * 交由 {@link WillMessage} 携带 —— 延迟投递的实际调度是独立课题（需要一个定时器,
     * 且要与会话过期回收的职责划分清楚), 当前不做, 但值必须解析出来, 否则会丢失语义。
     */
    private WillMessage extractWillMessage(MqttConnectMessage msg, ConnectOptions options) {
        if (!msg.variableHeader().isWillFlag()) {
            return null;
        }
        byte[] payload = msg.payload().willMessageInBytes();
        return new WillMessage(
                msg.payload().willTopic(),
                payload == null ? new byte[0] : payload,
                msg.variableHeader().willQos(),
                msg.variableHeader().isWillRetain(),
                options.willDelaySeconds());
    }

    /**
     * 重连后重发未确认的 PUBLISH(dup=1)与 PUBREL(dup=1)。
     */
    private void resendInflightMessages(Channel channel, String clientId) {
        List<DupPublishMessageStore> publishes = dupPublishMessageStoreService.get(clientId);
        for (DupPublishMessageStore stored : publishes) {
            channel.write(ReplyFactory.publish(channel, stored.topic(), stored.qos(),
                    stored.messageId(), stored.payload(), false, true));
        }
        List<DupPubRelMessageStore> pubRels = dupPubRelMessageStoreService.get(clientId);
        for (DupPubRelMessageStore stored : pubRels) {
            channel.write(ReplyFactory.pubRel(channel, stored.messageId(), ReplyFactory.SUCCESS));
        }
        if (!publishes.isEmpty() || !pubRels.isEmpty()) {
            log.debug("重发未确认消息 clientId={} publish={} pubrel={}",
                    clientId, publishes.size(), pubRels.size());
        }
        channel.flush();
    }
}
