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
import online.ipuff.jmqtt.store.IPendingMessageStore;
import online.ipuff.jmqtt.store.IDupPublishMessageStoreService;
import online.ipuff.jmqtt.store.IInboundQos2Store;
import online.ipuff.jmqtt.subscribe.ISubscribeStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 会话过期的级联清理。
 *
 * <p><b>为什么需要单独一个组件。</b>会话过期不只是「把会话从表里删掉」——
 * 它还挂着订阅关系、在途未确认消息、以及持久层的记录。只删会话的话:
 * <ul>
 *   <li>订阅会永远留在主题树里, 指向一个已不存在的客户端。
 *       主题树节点与订阅关系计数因此单调增长, 长期运行后
 *       匹配成本与内存都会被历史订阅拖垮。</li>
 *   <li>持久层记录会靠 TTL 自己过期, 但期间任何节点重连该 clientId 都会
 *       把它当成有效会话恢复, 实际上是「复活了一个应该被遗忘的会话」。</li>
 * </ul>
 *
 * <p>不放在 {@code SessionStoreService} 里, 是因为它需要清除订阅与持久层,
 * 而 {@code SessionPersistence} 又依赖 {@code ISessionStoreService} ——
 * 放进去会形成循环依赖。
 */
@Component
public class SessionExpiryReaper {

    private static final Logger log = LoggerFactory.getLogger(SessionExpiryReaper.class);

    private final ISessionStoreService sessionStoreService;
    private final ISubscribeStoreService subscribeStoreService;
    private final IDupPublishMessageStoreService dupPublishMessageStoreService;
    private final IDupPubRelMessageStoreService dupPubRelMessageStoreService;
    private final SessionPersistence sessionPersistence;
    private final IPendingMessageStore pendingMessageStore;
    private final IInboundQos2Store inboundQos2Store;

    public SessionExpiryReaper(ISessionStoreService sessionStoreService,
                               ISubscribeStoreService subscribeStoreService,
                               IDupPublishMessageStoreService dupPublishMessageStoreService,
                               IDupPubRelMessageStoreService dupPubRelMessageStoreService,
                               SessionPersistence sessionPersistence,
                               IPendingMessageStore pendingMessageStore,
                               IInboundQos2Store inboundQos2Store) {
        this.sessionStoreService = sessionStoreService;
        this.subscribeStoreService = subscribeStoreService;
        this.dupPublishMessageStoreService = dupPublishMessageStoreService;
        this.dupPubRelMessageStoreService = dupPubRelMessageStoreService;
        this.sessionPersistence = sessionPersistence;
        this.pendingMessageStore = pendingMessageStore;
        this.inboundQos2Store = inboundQos2Store;
    }

    /**
     * 周期性回收过期会话。
     *
     * <p>注意与遗嘱消息的区别: 遗嘱在<b>异常断连</b>时触发,
     * 与会话过期是两件互不相关的事, 不能靠这里来发遗嘱。
     */
    @Scheduled(fixedDelayString = "PT30S")
    public void reap() {
        List<String> expired = sessionStoreService.drainExpired();
        if (expired.isEmpty()) {
            return;
        }
        int subscriptions = 0;
        for (String clientId : expired) {
            subscriptions += subscribeStoreService.removeForClient(clientId);
            dupPublishMessageStoreService.removeByClient(clientId);
            dupPubRelMessageStoreService.removeByClient(clientId);
            // 会话真的过期了, 欠它的离线消息也没有意义了, 一并清掉
            pendingMessageStore.removeAll(clientId);
            // 会话没了, 它的接收方向 QoS 2 状态也没有意义了 —— 不清就会随
            // 「曾经连过的客户端数」单调增长
            inboundQos2Store.clearClient(clientId);
            sessionPersistence.sessionDestroyed(clientId);
        }
        log.info("回收过期会话 {} 个, 连带清除订阅 {} 条", expired.size(), subscriptions);
    }
}
