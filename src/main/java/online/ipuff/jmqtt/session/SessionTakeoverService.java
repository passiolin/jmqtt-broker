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
 */
@Service
public class SessionTakeoverService implements TakeoverListener {

    private static final Logger log = LoggerFactory.getLogger(SessionTakeoverService.class);

    private final ConnectionRegistry connectionRegistry;
    private final ISessionStoreService sessionStoreService;
    private final ISubscribeStoreService subscribeStoreService;
    private final IDupPublishMessageStoreService dupPublishMessageStoreService;
    private final IDupPubRelMessageStoreService dupPubRelMessageStoreService;
    private final IPendingMessageStore pendingMessageStore;
    private final IInboundQos2Store inboundQos2Store;
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
            // 本节点已不再持有该 clientId(可能刚自然断开, 或接管消息重复到达)
            log.debug("收到接管通知但本节点未持有该连接, 忽略: clientId={} 接管方={}",
                    clientId, fromNodeId);
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
        int subscriptions = subscribeStoreService.removeForClient(clientId);
        // detach 而非 removeByClient: 只清本节点内存, 持久镜像留给接管方节点加载。
        // 用 removeByClient 会把镜像一起删掉, 等于销毁新节点该投递的消息。
        dupPublishMessageStoreService.detach(clientId);
        dupPubRelMessageStoreService.removeByClient(clientId);
        // 接收方向的 QoS 2 状态是「这一轮流程在<b>本节点</b>进行到哪」, 不随会话迁移。
        // 留在本节点既无用(客户端已在别处)又会挡住将来同标识符的新流程
        inboundQos2Store.clearClient(clientId);

        if (inflight > 0) {
            // 内存态在这里结束, 但持久镜像已保留(detach): 客户端在接管方节点重连时,
            // 镜像被加载回来并以 dup=1 且原报文标识符重发 —— 消息本身并不丢。
            // 真正留下的缺口是 PUBREL 半程状态(上面已清): 服务端不会再主动重发未送达的 PUBREL,
            // 流程靠客户端超时重发 PUBREC/PUBREL 收敛, 新节点对两者都幂等。
            log.info("接管清理释放本节点在途状态 {} 条: clientId={}。持久镜像已保留, "
                            + "由接管方节点在客户端重连时加载重发",
                    inflight, clientId);
        }

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

        // 注意: 不触碰 SessionPersistence —— 会话此刻已归属接管方节点
        log.debug("接管清理完成: clientId={} 清除订阅 {} 条, 释放本节点在途 {} 条(镜像保留)",
                clientId, subscriptions, inflight);
    }
}
