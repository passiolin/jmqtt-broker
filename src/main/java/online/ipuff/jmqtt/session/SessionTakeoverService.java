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

import io.netty.channel.Channel;
import online.ipuff.jmqtt.cluster.TakeoverListener;
import online.ipuff.jmqtt.store.IInboundQos2Store;
import online.ipuff.jmqtt.store.IDupPubRelMessageStoreService;
import online.ipuff.jmqtt.store.IPendingMessageStore;
import online.ipuff.jmqtt.store.IDupPublishMessageStoreService;
import online.ipuff.jmqtt.subscribe.ISubscribeStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 跨节点连接接管的接收侧实现。
 *
 * <h2>刻意不依赖 SessionPersistence</h2>
 * 本类<b>绝不能</b>删除持久层的会话记录。收到接管通知时, 该会话已经归属新节点 ——
 * 新节点刚把它写进持久层, 这里删掉等于把刚建立的会话摧毁。
 * 为了从结构上杜绝这种错误, 本类不注入 {@code SessionPersistence}。
 *
 * <h2>关闭顺序有讲究</h2>
 * 必须<b>先移除会话, 再关闭连接</b>。反过来的话, 连接关闭会触发
 * {@code channelInactive}, 而那里会读取会话里的遗嘱消息并发布 ——
 * 但客户端只是换了节点, 并没有死, 发遗嘱是错的。
 * 先移除会话, {@code channelInactive} 读不到会话, 自然就不会发遗嘱。
 *
 * <h2>已发送未确认消息的去向</h2>
 * 内存清掉、持久镜像<b>刻意保留</b>(对 PUBLISH 在途走 {@code detach} 而非 {@code removeByClient}) ——
 * 客户端在接管方节点重连时, 镜像被加载回来, 以 {@code dup=1} 且原报文标识符重发。
 * 真正留下缺口的是 PUBREL 半程状态: 它不进镜像, 接管后服务端不会再重发未送达的 PUBREL。
 * 流程仍能收敛 —— 客户端库的超时策略会重发 PUBREC/PUBREL, 新节点对两者都幂等
 * (PUBREC 照常补出 PUBREL, PUBREL 照常回 PUBCOMP) —— 但依赖客户端超时, 不是服务端主动补发。
 * 另见接收方向 QoS 2 标识符集合的遗留说明({@code IInboundQos2Store})。
 */@Service
public class SessionTakeoverService implements TakeoverListener {

    private static final Logger log = LoggerFactory.getLogger(SessionTakeoverService.class);

    private final ConnectionRegistry connectionRegistry;
    private final ISessionStoreService sessionStoreService;
    private final ISubscribeStoreService subscribeStoreService;
    private final IInboundQos2Store inboundQos2Store;
    private final IDupPublishMessageStoreService dupPublishMessageStoreService;
    private final IDupPubRelMessageStoreService dupPubRelMessageStoreService;
    private final IPendingMessageStore pendingMessageStore;
    private final online.ipuff.jmqtt.metrics.NodeMetricsService nodeMetricsService;

    public SessionTakeoverService(ConnectionRegistry connectionRegistry,
                                  ISessionStoreService sessionStoreService,
                                  ISubscribeStoreService subscribeStoreService,
                                  IInboundQos2Store inboundQos2Store,
                                  IDupPublishMessageStoreService dupPublishMessageStoreService,
                                  IDupPubRelMessageStoreService dupPubRelMessageStoreService,
                                  IPendingMessageStore pendingMessageStore,
                                  online.ipuff.jmqtt.metrics.NodeMetricsService nodeMetricsService) {
        this.connectionRegistry = connectionRegistry;
        this.sessionStoreService = sessionStoreService;
        this.subscribeStoreService = subscribeStoreService;
        this.inboundQos2Store = inboundQos2Store;
        this.dupPublishMessageStoreService = dupPublishMessageStoreService;
        this.dupPubRelMessageStoreService = dupPubRelMessageStoreService;
        this.pendingMessageStore = pendingMessageStore;
        this.nodeMetricsService = nodeMetricsService;
    }

    @Override
    public void onTakeover(String clientId, String fromNodeId) {
        Channel channel = connectionRegistry.get(clientId);
        if (channel == null) {
            // 无活跃连接: 设备此前已从本节点断开, 但持久会话与订阅仍留在本节点
            // (保留至过期是 MQTT 语义)。此时会话归属已迁往接管方节点 ——
            // 若不清理本地副本, 本节点会在最长一个会话过期周期内继续为它
            // 入队离线消息, 与接管方节点双写。清理本地副本, 停止入队。
            if (sessionStoreService.get(clientId) == null) {
                // 本地既无连接也无会话: 接管消息重复到达, 忽略即可
                log.debug("收到接管通知但本节点未持有该连接与会话, 忽略: clientId={} 接管方={}",
                        clientId, fromNodeId);
                return;
            }
            log.info("收到跨节点接管通知: clientId={} 会话已迁至节点 [{}], "
                    + "本节点无活跃连接, 清理残留的会话与订阅副本", clientId, fromNodeId);
            cleanLocalRuntimeState(clientId, null);
            // 持久离线队列已随会话归属新节点, 本地不动
            return;
        }
        nodeMetricsService.takeoverRemote();

        int inflight = dupPublishMessageStoreService.get(clientId).size()
                + dupPubRelMessageStoreService.get(clientId).size();
        log.info("收到跨节点接管通知: clientId={} 已迁至节点 [{}], 释放本节点连接并清理本地状态",
                clientId, fromNodeId);

        // 1) 先移除会话 —— 这样随后的 channelInactive 不会误发遗嘱
        sessionStoreService.remove(clientId);

        // 2) 移出注册表, 使其不再可被投递
        connectionRegistry.unregister(clientId, channel);

        // 3) 关闭连接
        channel.close();

        // 4) 清理本地运行时状态
        cleanLocalRuntimeState(clientId, channel);

        // 5) 离线积压队列
        //    持久队列<b>不动</b> —— 它已随会话归属新节点, 删掉等于销毁新节点该投递的消息。
        //    非持久队列是本节点私有的, 客户端已迁走, 留在本节点也永远投不出去。
        if (!pendingMessageStore.persistent()) {
            int pending = pendingMessageStore.size(clientId);
            if (pending > 0) {
                log.warn("接管清理丢弃 {} 条离线积压消息(队列非持久, 无法随会话转移): clientId={}",
                        pending, clientId);
            }
            pendingMessageStore.removeAll(clientId);
        }

        log.info("接管清理完成: clientId={} 释放本节点在途 {} 条(持久镜像保留)",
                clientId, inflight);
    }

    /**
     * 清理本节点为某客户端保留的运行时状态(会话、订阅、在途、QoS2)。
     *
     * <p>持久会话的设备断开后, 会话与订阅会在本节点存活至过期 —— 这是 MQTT 语义。
     * 但一旦收到接管通知(设备已连到其他节点), 继续保留副本会让本节点持续为它
     * 入队离线消息, 与接管方节点双写。清理的是<b>本地副本</b>:
     * 会话与订阅的权威副本已由接管方节点从持久层恢复。
     *
     * @param channel 本节点上的活跃连接; 可为 null(设备早已断开的场景)
     */
    private void cleanLocalRuntimeState(String clientId, Channel channel) {
        // 先移除会话 —— 有连接时这样随后的 channelInactive 不会误发遗嘱
        sessionStoreService.remove(clientId);

        if (channel != null) {
            connectionRegistry.unregister(clientId, channel);
        }

        int subscriptions = subscribeStoreService.removeForClient(clientId);
        // detach 而非 removeByClient: 只清本节点内存, 持久镜像留给接管方节点加载。
        // 用 removeByClient 会把镜像一起删掉, 等于销毁新节点该投递的消息。
        dupPublishMessageStoreService.detach(clientId);
        dupPubRelMessageStoreService.removeByClient(clientId);
        // 接收方向的 QoS 2 状态是「这一轮流程在本节点进行到哪」, 不随会话迁移。
        // 留在本节点既无用(客户端已在别处)又会挡住将来同标识符的新流程
        inboundQos2Store.clearClient(clientId);
        log.debug("接管清理完成: clientId={} 清除订阅 {} 条", clientId, subscriptions);
    }
}