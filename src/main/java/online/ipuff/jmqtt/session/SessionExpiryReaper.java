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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 会话过期的周期性回收。
 *
 * <p><b>为什么需要单独一个组件。</b>会话过期不只是「把会话从表里删掉」——
 * 它还挂着订阅关系、在途未确认消息、离线积压、以及持久层的记录。只删会话的话:
 * <ul>
 *   <li>订阅会永远留在主题树里, 指向一个已不存在的客户端。
 *       主题树节点与订阅关系计数因此单调增长, 长期运行后
 *       匹配成本与内存都会被历史订阅拖垮。</li>
 *   <li>持久层记录会靠 TTL 自己过期, 但期间任何节点重连该 clientId 都会
 *       把它当成有效会话恢复, 实际上是「复活了一个应该被遗忘的会话」。</li>
 * </ul>
 * 级联清理统一走 {@link SessionLifecycle#destroy} —— 过期回收只是「什么时候销毁」,
 * 「销毁什么」只有一个答案。
 *
 * <p>过期判定在 {@link SessionStore#isExpired()}: 在线会话永远不过期,
 * 断开后从关闭时刻起算。所以这里回收的只会是真正离场的会话。
 */
@Component
public class SessionExpiryReaper {

    private static final Logger log = LoggerFactory.getLogger(SessionExpiryReaper.class);

    private final ISessionStoreService sessionStoreService;
    private final SessionLifecycle sessionLifecycle;

    public SessionExpiryReaper(ISessionStoreService sessionStoreService,
                               SessionLifecycle sessionLifecycle) {
        this.sessionStoreService = sessionStoreService;
        this.sessionLifecycle = sessionLifecycle;
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
        for (String clientId : expired) {
            sessionLifecycle.destroy(clientId);
        }
        log.info("回收过期会话 {} 个(含订阅/在途/离线积压级联清理)", expired.size());
    }
}
