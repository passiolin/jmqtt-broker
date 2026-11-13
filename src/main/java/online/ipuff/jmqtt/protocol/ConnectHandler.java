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
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.CharsetUtil;
import online.ipuff.jmqtt.admin.AdminStatePublisher;
import online.ipuff.jmqtt.auth.AuthResult;
import online.ipuff.jmqtt.auth.IAuthService;
import online.ipuff.jmqtt.cluster.ClusterBus;
import online.ipuff.jmqtt.cluster.InternalSendServer;
import online.ipuff.jmqtt.config.BrokerProperties;
import online.ipuff.jmqtt.handler.ChannelAttributes;
import online.ipuff.jmqtt.message.DupPubRelMessageStore;
import online.ipuff.jmqtt.message.DupPublishMessageStore;
import online.ipuff.jmqtt.message.SubscribeStore;
import online.ipuff.jmqtt.message.WillMessage;
import online.ipuff.jmqtt.metrics.NodeMetricsService;
import online.ipuff.jmqtt.session.BackpressureMetrics;
import online.ipuff.jmqtt.session.ConnectionRegistry;
import online.ipuff.jmqtt.session.ISessionStoreService;
import online.ipuff.jmqtt.session.SendBuffer;
import online.ipuff.jmqtt.session.SessionIoExecutor;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;

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
 * <h2>3.1.1 下仍然成立的三条约束</h2>
 * <ol>
 *   <li><b>回包形态不需要再记版本。</b>本分支只有 3.1.1 一种形态, CONNACK/PUBACK/
 *       UNSUBACK 的字节布局恒定, 不必把版本写到 Channel 上再逐处判断。</li>
 *   <li><b>「保留会话」判据就等价于 {@code cleanSession}。</b>3.1.1 没有
 *       Session Expiry Interval, {@code cleanSession=0} 即「断开后保留」。
 *       代码仍走 {@link SessionStore#isPersistent()}, 但值为 0 只会来自
 *       {@code cleanSession=1}。</li>
 *   <li><b>在途窗口只受服务端 {@code max-inflight} 约束。</b>3.1.1 没有客户端侧的
 *       Receive Maximum 协商。</li>
 * </ol>
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
    private final NodeMetricsService nodeMetricsService;
    private final online.ipuff.jmqtt.session.SessionLifecycle sessionLifecycle;
    /** Redis 会话 I/O 执行器; 为 null 表示同步路径(测试/未启用 Redis) */
    private final SessionIoExecutor sessionIo;

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
                          IInboundQos2Store inboundQos2Store,
                          NodeMetricsService nodeMetricsService,
                          online.ipuff.jmqtt.session.SessionLifecycle sessionLifecycle) {
        this(properties, authService, sessionStoreService, subscribeStoreService,
                dupPublishMessageStoreService, dupPubRelMessageStoreService, connectionRegistry,
                sessionPersistence, clusterBus, internalSendServer, backpressureMetrics,
                inflightPersistence, adminStatePublisher, inboundQos2Store, nodeMetricsService,
                sessionLifecycle, null);
    }

    @Autowired
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
                          IInboundQos2Store inboundQos2Store,
                          NodeMetricsService nodeMetricsService,
                          online.ipuff.jmqtt.session.SessionLifecycle sessionLifecycle,
                          SessionIoExecutor sessionIo) {
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
        this.nodeMetricsService = nodeMetricsService;
        this.sessionLifecycle = sessionLifecycle;
        this.sessionIo = sessionIo;
    }

    public void processConnect(Channel channel, MqttConnectMessage msg) {
        // 本分支服务 3.1 与 3.1.1 两个协议级别 —— 它们在报文层面几乎一致
        // (payload 结构相同, 各类回包字节布局相同), 差别只在 3.1 的 clientId 限 23 字符、
        // 以及 CONNACK bit0 的语义(那一位我们恒为 0, 两种解读下都不产生误判)。
        // 详情见 {@link ConnectOptions}。
        //
        // 真正走不到这里的版本只剩 v5.0。对应用户诉求:「功能不支持, 但不要强硬拒绝」——
        // 只有「连上之后回包形态必然错位」才值得在握手阶段拒, 其余一律放行。
        //
        // 其余非法协议级别(级别与协议名不匹配、未知级别)由 Netty 解码器抛出
        // MqttUnacceptableProtocolVersionException 拦下, 走 handleDecodeFailure。
        int protocolLevel = msg.variableHeader().version();
        if (protocolLevel != ConnectOptions.VERSION_3_1
                && protocolLevel != ConnectOptions.VERSION_3_1_1) {
            log.debug("CONNECT 被拒绝: 不支持的协议级别 {} (支持 3.1 / 3.1.1)", protocolLevel);
            nodeMetricsService.connectionRejected("protocol");
            rejectAndClose(channel, ReplyFactory.ConnAckReason.UNSUPPORTED_PROTOCOL_VERSION);
            return;
        }

        ConnectOptions options = ConnectOptions.parse(msg, properties);
        channel.attr(ChannelAttributes.CONNECT_OPTIONS).set(options);
        // 用户名在认证前后各有一个读者(HTTP 认证中心 / ACL 服务), 统一落到 Channel 上
        channel.attr(ChannelAttributes.USERNAME).set(msg.payload().userName());

        String clientId = msg.payload().clientIdentifier();

        // 1) clientId: 3.1.1 没有服务端分配机制, 空 clientId 只能拒绝
        if (clientId == null || clientId.isEmpty()) {
            log.debug("CONNECT 被拒绝: clientId 为空");
            nodeMetricsService.connectionRejected("clientId");
            rejectAndClose(channel, ReplyFactory.ConnAckReason.CLIENT_IDENTIFIER_NOT_VALID);
            return;
        }

        // 2) 认证 —— 可能要等外部 HTTP 认证中心的响应, 挂起处理。
        //    挂起前先把续接要用到的 CONNECT 数据全部取出:
        //    SimpleChannelInboundHandler 会在本方法返回后释放报文,
        //    而 CONNECT 的 payload 在解码时已物化为 byte[]/String, 现在取是安全的。
        WillMessage willMessage = extractWillMessage(msg, options);
        int requestedKeepAlive = msg.variableHeader().keepAliveTimeSeconds();
        String username = msg.payload().userName();
        String password = msg.payload().passwordInBytes() == null
                ? null
                : new String(msg.payload().passwordInBytes(), CharsetUtil.UTF_8);

        CompletableFuture<AuthResult> authFuture = properties.authEnabled()
                ? authService.authenticate(clientId, username, password,
                        ChannelAttributes.peerhostOf(channel))
                : CompletableFuture.completedFuture(AuthResult.ALLOW);

        awaitAuth(channel, authFuture, () -> finishConnect(
                channel, options, clientId, willMessage, requestedKeepAlive));
    }

    /**
     * 等认证结果, 再执行续接。
     *
     * <p>future 已完成(内置静态鉴权 / 鉴权关闭)时<b>同步内联</b>执行 ——
     * HTTP 鉴权未开启的部署行为与异步化之前完全一致, 不付任何线程切换成本。
     * 未完成(HTTP 认证挂起中)时置 {@link ChannelAttributes#AUTH_PENDING},
     * 入口据此忽略后续报文; 结果回来后 hop 回该连接的 EventLoop 续接,
     * 连接状态始终保持单线程访问。挂起期间连接断开则安全放弃。
     */
    private void awaitAuth(Channel channel, CompletableFuture<AuthResult> future,
                           Runnable continuation) {
        if (future.isDone()) {
            completeAuth(channel, join(future), continuation);
            return;
        }
        channel.attr(ChannelAttributes.AUTH_PENDING).set(Boolean.TRUE);
        future.whenComplete((result, error) -> channel.eventLoop().execute(() -> {
            channel.attr(ChannelAttributes.AUTH_PENDING).set(null);
            if (!channel.isActive()) {
                log.debug("认证结果到达前连接已断开, 放弃本次 CONNECT");
                return;
            }
            completeAuth(channel, error == null ? result : AuthResult.DENY, continuation);
        }));
    }

    /**
     * 已完成的 future 取值; 异常一律按拒绝处理(实现方本不该让 future 异常完成,
     * 这里是兜底 —— 认证路径上的任何意外都不能变成放行)。
     */
    private static AuthResult join(CompletableFuture<AuthResult> future) {
        try {
            return future.join();
        } catch (RuntimeException e) {
            return AuthResult.DENY;
        }
    }

    private void completeAuth(Channel channel, AuthResult result, Runnable continuation) {
        if (!result.allowed()) {
            log.debug("CONNECT 被拒绝: 认证失败");
            nodeMetricsService.connectionRejected("auth");
            rejectAndClose(channel, ReplyFactory.ConnAckReason.BAD_CREDENTIALS);
            return;
        }
        if (result.superuser()) {
            channel.attr(ChannelAttributes.SUPERUSER).set(Boolean.TRUE);
        }
        continuation.run();
    }

    /**
     * 认证通过后的 CONNECT 后半段: 会话处理、落库、注册、CONNACK、恢复投递。
     * 参数里的遗嘱与心跳是挂起前从 CONNECT 报文里取出的快照。
     */
    /**
     * 认证通过后的 CONNECT 后半段。
     *
     * <p>持久会话走异步会话 I/O(Redis 不占 event loop); 其余情况保持原有同步路径 ——
     * 未启用 Redis 的部署没有阻塞来源, 不值得为此多一次线程切换。
     */
    private void finishConnect(Channel channel, ConnectOptions options, String clientId,
                               WillMessage willMessage, int requestedKeepAlive) {
        if (properties.redis().enabled() && sessionIo != null && !options.cleanSession()) {
            finishConnectAsync(channel, options, clientId, willMessage, requestedKeepAlive);
            return;
        }
        finishConnectSync(channel, options, clientId, willMessage, requestedKeepAlive);
    }

    private void finishConnectSync(Channel channel, ConnectOptions options, String clientId,
                                   WillMessage willMessage, int requestedKeepAlive) {
        // 3) 会话处理 —— 决定 sessionPresent
        SessionStore existing = sessionStoreService.get(clientId);
        boolean sessionPresent = false;

        if (options.cleanSession()) {
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

        // 4) 遗嘱已在挂起前解析(参数传入)

        // 5) 落库会话
        SessionStore session = (existing != null && !options.cleanSession())
                ? existing
                : new SessionStore(properties.id(), clientId, options.cleanSession(),
                        options.sessionExpirySeconds());
        session.setExpireSeconds(options.sessionExpirySeconds());
        session.setWillMessage(willMessage);
        // 撤销过期判定: 连接在, 会话就不过期(过期时钟从断开起算)
        session.markOpen();
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

        // 6) 心跳: 3.1.1 没有 Server Keep Alive 属性, 服务端无法改写客户端声明的值 ——
        //    只能沿用 CONNECT 里的协商结果。要收紧死连接占用, 得靠服务端自己的
        //    空闲检测(见 configureKeepAlive 里的宽限系数), 而不是改这个值。
        int keepAlive = requestedKeepAlive;
        configureKeepAlive(channel, keepAlive);

        // 7) 注册连接, 并踢掉同一 clientId 的旧连接(连接接管)
        channel.attr(ChannelAttributes.CLIENT_ID).set(clientId);
        // 管理面要展示「接入时间 / 心跳 / 协议版本」, 前两项在这里落一次即可 ——
        // 之后它们在连接生命周期内不再变化, 读取方无需回查会话或重解析 CONNECT
        channel.attr(ChannelAttributes.CONNECTED_AT).set(System.currentTimeMillis());
        channel.attr(ChannelAttributes.KEEP_ALIVE).set(keepAlive);
        // 发送缓冲挂在 Channel 上: 它的生命周期就是这条连接, 随连接自动回收。
        // 必须在任何投递发生之前装配好, 否则投递会走「未装配缓冲」的退化路径。
        // 窗口只由服务端 max-inflight 决定 —— 3.1.1 没有客户端侧的流控协商
        int window = Math.max(1, properties.maxInflight());
        channel.attr(ChannelAttributes.SEND_BUFFER).set(new SendBuffer(
                window, properties.maxMqueueLen(), backpressureMetrics));
        Channel previous = connectionRegistry.register(clientId, channel);
        if (previous != null && previous != channel) {
            log.info("clientId={} 发生连接接管, 关闭旧连接 {}", clientId, previous.id().asShortText());
            nodeMetricsService.takeoverLocal();
            previous.close();
        }

        // 注册后复查会话归属: 从 put 会话到注册之间隔着一次 Redis 往返(acquireOwnership),
        // 跨节点接管通知恰好落在这个窗口时, 会把刚恢复的会话与订阅当残留清掉。
        // 不复查就注册上一条「活连接 + 零订阅」—— 客户端心跳正常却永远收不到消息。
        if (sessionStoreService.get(clientId) != session) {
            log.warn("clientId={} 的会话在注册间隙被接管清理, 断开本次连接让客户端重连", clientId);
            connectionRegistry.unregister(clientId, channel);
            channel.close();
            return;
        }

        // 8) CONNACK
        channel.writeAndFlush(ReplyFactory.connAck(
                MqttConnectReturnCode.CONNECTION_ACCEPTED, sessionPresent));
        log.debug("CONNECT 成功 clientId={} cleanSession={} sessionPresent={} 会话保留={}s 窗口={}",
                clientId, options.cleanSession(), sessionPresent,
                session.getExpireSeconds(), window);

        // 管理面可见性: 只往有界队列里塞一条, 不做任何 Redis 调用。
        // 放在这里而不是更早, 是因为它要读到连接上的全部属性(版本/心跳/时间/发送缓冲)
        // 以及已恢复的订阅 —— 早于恢复订阅就会上报成「零订阅」。
        adminStatePublisher.clientOnline(clientId, channel);
        nodeMetricsService.connectionOpened();
        // 连接事件(上线): key=clientId 有序, 只入有界队列不阻塞连接路径
        clusterBus.publishConnectionEvent(online.ipuff.jmqtt.cluster.ConnectionEvent.connected(
                clientId,
                channel.attr(ChannelAttributes.USERNAME).get(),
                4,
                ChannelAttributes.peernameOf(channel),
                properties.id()));

        // 9) 恢复投递: 在途重发 + 离线积压。
        //    门控是「服务端确实持有旧会话」而不是「会话是否持久」——
        //    cleanSession=0 时只要服务端还有会话状态就必须恢复,
        //    SEI 只决定连接关闭后会话活多久([MQTT-3.1.2-4])。
        //    用 isPersistent() 门控会让这类客户端拿到 sessionPresent=true 却什么都收不到。
        if (sessionPresent) {
            loadInflightFromMirror(clientId);
            resendInflightMessages(channel, clientId);

            // 10) 投递离线积压 —— 客户端离线期间本应收到但没能送达的 QoS 1/2 消息。
            //     放在重发未确认消息之后: 先补齐已发出的, 再补未发出的, 顺序更贴近真实时序。
            internalSendServer.deliverPendingMessages(clientId, channel);
        }
    }

    /**
     * 持久会话的异步接入: restore + 取归属 + 载入在途镜像在会话 I/O 线程完成,
     * event loop 只做状态机决策与 Netty 写。
     *
     * <p>挂起期间 {@link ChannelAttributes#AUTH_PENDING} 保持 true, 入口会忽略后续报文;
     * 保存会话完成后再回 event loop 发 CONNACK —— 与同步路径「先落库再确认」的
     * 持久性语义一致。
     */
    private void finishConnectAsync(Channel channel, ConnectOptions options, String clientId,
                                    WillMessage willMessage, int requestedKeepAlive) {
        SessionStore existing = sessionStoreService.get(clientId);
        long expireSeconds = options.sessionExpirySeconds();
        channel.attr(ChannelAttributes.AUTH_PENDING).set(Boolean.TRUE);

        sessionIo.submit(clientId, () -> {
            // 本进程没有时才读持久层; 有本地会话就不必多一次 Redis 往返
            PersistedSession persisted = existing == null ? sessionPersistence.restore(clientId) : null;
            // 归属必须在写会话之前取: acquireOwner 会读出旧 owner
            String previousOwner = sessionPersistence.acquireOwnership(
                    clientId, properties.id(), expireSeconds);
            List<DupPublishMessageStore> inflight = (existing != null || persisted != null)
                    ? inflightPersistence.load(clientId) : List.of();
            return new PreparedSession(persisted, previousOwner, inflight);
        }).whenComplete((prepared, error) -> {
            if (error != null) {
                log.warn("会话持久层准备失败, 按新会话继续: clientId={}", clientId, error);
            }
            PreparedSession state = error == null ? prepared : PreparedSession.EMPTY;
            channel.eventLoop().execute(() -> {
                if (!channel.isActive()) {
                    channel.attr(ChannelAttributes.AUTH_PENDING).set(null);
                    return;
                }
                continueAsyncConnect(channel, options, clientId, willMessage, requestedKeepAlive,
                        existing, state);
            });
        });
    }

    /** 异步接入的 event-loop 段: 建会话/恢复订阅/注册, 然后把落库交给会话 I/O 线程。 */
    private void continueAsyncConnect(Channel channel, ConnectOptions options, String clientId,
                                      WillMessage willMessage, int requestedKeepAlive,
                                      SessionStore existing, PreparedSession prepared) {
        boolean sessionPresent = false;
        if (prepared.persisted() != null) {
            PersistedSession persisted = prepared.persisted();
            existing = new SessionStore(properties.id(), clientId, false,
                    persisted.expireSeconds() > 0 ? persisted.expireSeconds() : properties.sessionExpirySeconds(),
                    null, persisted.createdAt());
            restoreSubscriptions(persisted);
            sessionPresent = true;
        } else if (existing != null) {
            sessionPresent = true;
        } else {
            clearDanglingState(clientId);
        }

        SessionStore session = existing != null
                ? existing
                : new SessionStore(properties.id(), clientId, false, options.sessionExpirySeconds());
        session.setExpireSeconds(options.sessionExpirySeconds());
        session.setWillMessage(willMessage);
        session.markOpen();
        sessionStoreService.put(session);

        if (prepared.previousOwner() != null && !prepared.previousOwner().equals(properties.id())) {
            announceTakeover(clientId, prepared.previousOwner());
        }

        int keepAlive = requestedKeepAlive;
        configureKeepAlive(channel, keepAlive);
        channel.attr(ChannelAttributes.CLIENT_ID).set(clientId);
        channel.attr(ChannelAttributes.CONNECTED_AT).set(System.currentTimeMillis());
        channel.attr(ChannelAttributes.KEEP_ALIVE).set(keepAlive);
        int window = Math.max(1, properties.maxInflight());
        channel.attr(ChannelAttributes.SEND_BUFFER).set(new SendBuffer(
                window, properties.maxMqueueLen(), backpressureMetrics));
        Channel previous = connectionRegistry.register(clientId, channel);
        if (previous != null && previous != channel) {
            log.info("clientId={} 发生连接接管, 关闭旧连接 {}", clientId, previous.id().asShortText());
            nodeMetricsService.takeoverLocal();
            previous.close();
        }

        if (sessionStoreService.get(clientId) != session) {
            log.warn("clientId={} 的会话在注册间隙被接管清理, 断开本次连接让客户端重连", clientId);
            connectionRegistry.unregister(clientId, channel);
            channel.attr(ChannelAttributes.AUTH_PENDING).set(null);
            channel.close();
            return;
        }

        final boolean present = sessionPresent;
        sessionIo.execute(clientId, () -> sessionPersistence.saveSession(session))
                .whenComplete((ignored, error) -> channel.eventLoop().execute(() -> {
                    if (error != null) {
                        log.warn("写入持久会话失败, 继续接入: clientId={}", clientId, error);
                    }
                    channel.attr(ChannelAttributes.AUTH_PENDING).set(null);
                    if (!channel.isActive()) {
                        return;
                    }
                    completeAsyncConnect(channel, clientId, options, session, present,
                            keepAlive, window, prepared.inflight());
                }));
    }

    /** 异步接入的收尾: CONNACK、管理面/事件、在途重发与离线积压。 */
    private void completeAsyncConnect(Channel channel, String clientId, ConnectOptions options,
                                      SessionStore session, boolean sessionPresent,
                                      int keepAlive, int window,
                                      List<DupPublishMessageStore> inflight) {
        channel.writeAndFlush(ReplyFactory.connAck(
                MqttConnectReturnCode.CONNECTION_ACCEPTED, sessionPresent));
        log.debug("CONNECT 成功 clientId={} cleanSession={} sessionPresent={} 会话保留={}s 窗口={}",
                clientId, options.cleanSession(), sessionPresent,
                session.getExpireSeconds(), window);

        adminStatePublisher.clientOnline(clientId, channel);
        nodeMetricsService.connectionOpened();
        clusterBus.publishConnectionEvent(online.ipuff.jmqtt.cluster.ConnectionEvent.connected(
                clientId,
                channel.attr(ChannelAttributes.USERNAME).get(),
                4,
                ChannelAttributes.peernameOf(channel),
                properties.id()));

        if (sessionPresent) {
            for (DupPublishMessageStore message : inflight) {
                dupPublishMessageStoreService.put(message);
            }
            resendInflightMessages(channel, clientId);
            internalSendServer.deliverPendingMessages(clientId, channel);
        }
    }

    /** 会话 I/O 线程产出、event loop 消费的接入准备结果。 */
    private record PreparedSession(PersistedSession persisted, String previousOwner,
                                   List<DupPublishMessageStore> inflight) {

        static final PreparedSession EMPTY = new PreparedSession(null, null, List.of());
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
     * 提示旧节点释放该 clientId 的连接。
     *
     * <p>MQTT 规范要求: 同一 clientId 已有连接时, 服务端必须断开既有连接
     * （[MQTT-3.1.4-2]）。
     * 单节点内这件事由 {@code ConnectionRegistry.register} 顺手完成; 跨节点则靠
     * <b>连接事件流</b>——本次 CONNECT 随后发布的 {@code connected} 事件会带上
     * 本节点 id, 旧节点消费到后自行释放。这里只负责把「效果有没有」讲清楚。
     */
    private void announceTakeover(String clientId, String previousOwner) {
        if (clusterBus.takeoverSupported()) {
            log.info("clientId={} 的会话归属在节点 [{}], 已通过连接事件触发其释放旧连接(跨节点接管)",
                    clientId, previousOwner);
            return;
        }
        log.warn("clientId={} 的会话归属在节点 [{}], 但跨节点接管未启用"
                        + "(需 cluster-enabled=true 且配置 kafka.connection-event-topic)。"
                        + "旧连接会存活到自身心跳超时, 期间该客户端实际处于双连状态。",
                clientId, previousOwner);
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

    /**
     * 用对应版本的拒绝码回 CONNACK 并关闭连接。
     */
    private void rejectAndClose(Channel channel, ReplyFactory.ConnAckReason reason) {
        channel.writeAndFlush(ReplyFactory.connAck(ReplyFactory.connAckCode(reason), false))
                .addListener(f -> channel.close());
    }

    /**
     * 清除会话及其全部附属状态, 包含持久层。
     */
    private void clearSessionState(String clientId) {
        // 级联清单只有一个来源(SessionLifecycle.destroy) ——
        // 各处自写清单就是各种泄漏的温床。force: cleanStart 是客户端明确要求
        // 丢弃既有会话, 持久层记录无条件删除
        sessionLifecycle.destroy(clientId, true);
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
     * 构造遗嘱消息。3.1.1 的遗嘱没有延迟投递语义: 异常断连即刻发布。
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
                msg.variableHeader().isWillRetain());
    }

    /**
     * 重连后重发未确认的 PUBLISH(dup=1)与 PUBREL(dup=1)。
     *
     * <p>重发的 PUBLISH 必须<b>占发送窗口</b>(登记进 {@link SendBuffer} 的在途账本):
     * 它们就是未确认的 QoS 1/2, 不占窗口就会与随后的新消息一起突破客户端的
     * Receive Maximum; 不登记则确认到达时窗口无从释放。
     * PUBREL 不占窗口 —— 它闭环的是 PUBREL 流程, 见 {@code PubRecHandler}。
     */
    private void resendInflightMessages(Channel channel, String clientId) {
        SendBuffer buffer = channel.attr(ChannelAttributes.SEND_BUFFER).get();
        List<DupPublishMessageStore> publishes = dupPublishMessageStoreService.get(clientId);
        for (DupPublishMessageStore stored : publishes) {
            channel.write(ReplyFactory.publish(stored.topic(), stored.qos(),
                    stored.messageId(), stored.payload(), false, true));
            if (buffer != null) {
                buffer.confirm(stored.messageId());
            }
        }
        List<DupPubRelMessageStore> pubRels = dupPubRelMessageStoreService.get(clientId);
        for (DupPubRelMessageStore stored : pubRels) {
            channel.write(ReplyFactory.pubRel(stored.messageId()));
        }
        if (!publishes.isEmpty() || !pubRels.isEmpty()) {
            log.debug("重发未确认消息 clientId={} publish={} pubrel={}",
                    clientId, publishes.size(), pubRels.size());
        }
        channel.flush();
    }
}
