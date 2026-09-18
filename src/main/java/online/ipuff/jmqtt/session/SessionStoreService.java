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
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 会话存储的进程内实现(P0 基线)。
 *
 * 现阶段改为进程内 {@link ConcurrentHashMap}, 目的有两个:
 * <ol>
 *   <li>让项目无需外部依赖即可启动跑通</li>
 *   <li>把「投递路径上访问外部存储」这个性能陷阱彻底拿掉</li>
 * </ol>
 *
 * <p><b>下一步: 接入 Redis 做跨节点会话恢复。</b>届时本类替换为 Redis 实现,
 * 接口不变。注意两点约束(详见设计文档):
 * <ul>
 *   <li>会话记录<b>必须包含订阅关系</b> —— MQTT 规范中订阅是 session state 的一部分。
 *       若不含, 重连时只能回 {@code sessionPresent=false}。</li>
 *   <li>Redis 只在「连接 / 重连 / 接管 / 订阅变更」这类低频路径上读写,
 *       <b>绝不进入消息投递路径</b>。</li>
 * </ul>
 */
@Service
public class SessionStoreService implements ISessionStoreService {

    private static final Logger log = LoggerFactory.getLogger(SessionStoreService.class);

    private final ConcurrentHashMap<String, SessionStore> sessions = new ConcurrentHashMap<>();

    @Override
    public void put(SessionStore session) {
        sessions.put(session.getClientId(), session);
    }

    @Override
    public SessionStore get(String clientId) {
        SessionStore session = sessions.get(clientId);
        if (session == null) {
            return null;
        }
        if (session.isExpired()) {
            sessions.remove(clientId, session);
            return null;
        }
        return session;
    }

    @Override
    public boolean containsKey(String clientId) {
        return get(clientId) != null;
    }

    @Override
    public void remove(String clientId) {
        sessions.remove(clientId);
    }

    @Override
    public int size() {
        return sessions.size();
    }

    @Override
    public Collection<SessionStore> all() {
        return new ArrayList<>(sessions.values());
    }

    @Override
    public List<String> drainExpired() {
        List<String> expired = new ArrayList<>();
        for (SessionStore session : sessions.values()) {
            if (session.isExpired()) {
                expired.add(session.getClientId());
            }
        }
        // 再次确认后再移除, 避免与并发的 touch() 竞争
        List<String> drained = new ArrayList<>(expired.size());
        for (String clientId : expired) {
            SessionStore session = sessions.get(clientId);
            if (session != null && session.isExpired() && sessions.remove(clientId, session)) {
                drained.add(clientId);
            }
        }
        return drained;
    }
}
