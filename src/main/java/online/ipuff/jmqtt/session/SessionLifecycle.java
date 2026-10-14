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
package online.ipuff.jmqtt.session;

import online.ipuff.jmqtt.session.persistence.SessionPersistence;
import online.ipuff.jmqtt.store.IDupPubRelMessageStoreService;
import online.ipuff.jmqtt.store.IDupPublishMessageStoreService;
import online.ipuff.jmqtt.store.IInboundQos2Store;
import online.ipuff.jmqtt.store.IMessageIdService;
import online.ipuff.jmqtt.store.IPendingMessageStore;
import online.ipuff.jmqtt.store.OfflineEnqueueBuffer;
import online.ipuff.jmqtt.subscribe.ISubscribeStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 会话生命周期的<b>唯一</b>收口: 连接关闭时决定会话去留, 以及会话的彻底销毁。
 *
 * <h2>为什么必须收口</h2>
 * 会话销毁原本散在四处(优雅 DISCONNECT / cleanStart 重连 / 过期回收 / 接管清理),
 * 各写各的级联清理。漏一处就是一种泄漏: 曾经漏掉「异常断连」这条最常用的路径,
 * 于是 {@code cleanSession=1} 的设备掉线后会话、订阅、在途状态永久残留,
 * 主题树被历史订阅拖着做无用匹配, 而回收器对这类会话无能为力
 * (保留时长为 0 的会话没有「过期」一说)。现在所有去留决策都从这里走。
 *
 * <h2>连接关闭({@link #connectionClosed})的判据</h2>
 * <ul>
 *   <li>持久会话(保留时长 &gt; 0): 保留, 记录关闭时刻 —— 过期从这一刻起算</li>
 *   <li>非持久会话: 立即彻底销毁([MQTT-3.1.2-6]: clean session 随网络连接结束)</li>
 * </ul>
 *
 * <p><b>调用方约束</b>: 只有「该连接是这个 clientId 的当前注册连接」时才能调用
 * (即 {@code ConnectionRegistry.unregister} 的值语义返回 true)。被接管的旧连接
 * 关闭时绝不能碰会话 —— 会话此刻归新连接所有。
 */
@Component
public class SessionLifecycle {

    private static final Logger log = LoggerFactory.getLogger(SessionLifecycle.class);

    private final ISessionStoreService sessionStoreService;
    private final ISubscribeStoreService subscribeStoreService;
    private final IDupPublishMessageStoreService dupPublishMessageStoreService;
    private final IDupPubRelMessageStoreService dupPubRelMessageStoreService;
    private final IInboundQos2Store inboundQos2Store;
    private final IPendingMessageStore pendingMessageStore;
    private final OfflineEnqueueBuffer offlineEnqueueBuffer;
    private final IMessageIdService messageIdService;
    private final SessionPersistence sessionPersistence;

    public SessionLifecycle(ISessionStoreService sessionStoreService,
                            ISubscribeStoreService subscribeStoreService,
                            IDupPublishMessageStoreService dupPublishMessageStoreService,
                            IDupPubRelMessageStoreService dupPubRelMessageStoreService,
                            IInboundQos2Store inboundQos2Store,
                            IPendingMessageStore pendingMessageStore,
                            OfflineEnqueueBuffer offlineEnqueueBuffer,
                            IMessageIdService messageIdService,
                            SessionPersistence sessionPersistence) {
        this.sessionStoreService = sessionStoreService;
        this.subscribeStoreService = subscribeStoreService;
        this.dupPublishMessageStoreService = dupPublishMessageStoreService;
        this.dupPubRelMessageStoreService = dupPubRelMessageStoreService;
        this.inboundQos2Store = inboundQos2Store;
        this.pendingMessageStore = pendingMessageStore;
        this.offlineEnqueueBuffer = offlineEnqueueBuffer;
        this.messageIdService = messageIdService;
        this.sessionPersistence = sessionPersistence;
    }

    /**
     * 连接关闭(异常断连)时决定会话去留。
     *
     * <p>优雅 DISCONNECT 的对应逻辑在 {@code DisconnectHandler} ——
     * 那里还要处理 v5 的保留时长改写与遗嘱保留, 收口点仍是本方法。
     */
    public void connectionClosed(String clientId) {
        SessionStore session = sessionStoreService.get(clientId);
        if (session == null) {
            return;
        }
        if (session.isPersistent()) {
            session.markClosed();
            sessionPersistence.saveSession(session);
            log.debug("连接关闭, 会话保留 {}s: clientId={}", session.getExpireSeconds(), clientId);
        } else {
            log.debug("连接关闭, 非持久会话即刻销毁: clientId={}", clientId);
            destroy(clientId);
        }
    }

    /**
     * 彻底销毁会话及其全部附属状态: 内存 + 离线积压(含未刷盘缓冲) + 持久层。
     *
     * <p>幂等: 重复调用无副作用。持久层删除<b>带归属校验</b> —— 本节点的陈旧副本
     * 过期时不能把别的节点正在服务的会话记录删掉。
     */
    public void destroy(String clientId) {
        destroy(clientId, false);
    }

    /**
     * 彻底销毁会话。
     *
     * @param force {@code true} = 无条件处置持久数据(客户端 cleanStart 明确要求
     *              丢弃既有会话); {@code false} = 先做归属校验, 归属已迁走时
     *              只清本地内存副本, 共享持久数据(在途镜像/离线队列)归新节点
     */
    public void destroy(String clientId, boolean force) {
        boolean owned;
        if (force) {
            sessionPersistence.sessionDiscarded(clientId);
            owned = true;
        } else {
            owned = sessionPersistence.sessionDestroyed(clientId);
        }
        sessionStoreService.remove(clientId);
        subscribeStoreService.removeForClient(clientId);
        // 会话没了, 它的接收方向 QoS 2 状态也没有意义了 —— 不清就会随
        // 「曾经连过的客户端数」单调增长
        inboundQos2Store.clearClient(clientId);
        // PUBREL 半程状态只在内存, 无镜像可留
        dupPubRelMessageStoreService.removeByClient(clientId);
        // 报文标识符计数器随会话释放
        messageIdService.release(clientId);
        if (owned) {
            dupPublishMessageStoreService.removeByClient(clientId); // 含持久镜像删除
            // 离线积压两处都要清: 未刷盘的缓冲尾巴只在缓冲里,
            // 只清存储会把它们留给下一个同 clientId 的会话
            offlineEnqueueBuffer.discard(clientId);
            pendingMessageStore.removeAll(clientId);
        } else {
            // 归属已迁走(接管竞态/通知迟到): 与接管清理同口径 ——
            // 本地内存照清, 镜像留给新节点加载, 缓冲刷进存储交给他投递
            dupPublishMessageStoreService.detach(clientId);
            offlineEnqueueBuffer.flushClient(clientId);
            log.info("会话 {} 的归属已迁离本节点, 只清本地副本, 持久数据留给新归属方", clientId);
        }
    }
}
