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
package online.ipuff.jmqtt.store;

import online.ipuff.jmqtt.message.DupPublishMessageStore;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * QoS 1/2 未确认 PUBLISH 的进程内存储, 带持久镜像。
 *
 * <p>进程内 {@link ConcurrentHashMap} 始终是<b>投递路径的真源</b> —— 读写都在内存里,
 * 延迟不受 Redis 影响。持久镜像({@link InflightPersistence})只是它的可恢复副本,
 * 在变更后异步或同步地跟上。
 *
 * <p><b>为什么不做成「Redis 直读直写」</b>: 那样每条 QoS 1/2 的投递和确认都要走一次
 * 网络往返, 直接压在投递热路径上。镜像模式让投递路径保持纯内存,
 * 代价是可恢复性有一个刷盘窗口(见 {@link InflightPersistence.Mode})。
 *
 * <p>每次变更都向镜像传<b>变化后的完整快照</b>: 整份覆盖天然幂等、无顺序问题,
 * 而每个客户端的在途集合又有 {@code max-inflight} 的硬上限, 成本可控。
 */
@Service
public class DupPublishMessageStoreService implements IDupPublishMessageStoreService {

    /** clientId -> (messageId -> 消息) */
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, DupPublishMessageStore>> stores =
            new ConcurrentHashMap<>();

    private final ObjectProvider<InflightPersistence> persistence;

    /**
     * 由 Spring 注入持久镜像。
     *
     * <p><b>必须显式标 {@code @Autowired}。</b>本类有两个构造函数, 而 Spring 在
     * 多构造函数且无注解时会**静默选择无参的那个** —— 后果是 {@code persistence}
     * 永远为 null, 镜像静默不写, 且不报任何错。这个坑值得单独标出来。
     */
    @Autowired
    public DupPublishMessageStoreService(ObjectProvider<InflightPersistence> persistence) {
        this.persistence = persistence;
    }

    /**
     * 不挂持久镜像的实例。
     *
     * <p>用于两种场景: 未启用 Redis(镜像本来就不存在), 以及单元测试
     * —— 被测的是内存语义, 不该被持久化牵进来。
     */
    public DupPublishMessageStoreService() {
        this.persistence = null;
    }

    @Override
    public void put(DupPublishMessageStore message) {
        stores.computeIfAbsent(message.clientId(), k -> new ConcurrentHashMap<>())
                .put(message.messageId(), message);
        mirror(message.clientId());
    }

    @Override
    public List<DupPublishMessageStore> get(String clientId) {
        ConcurrentHashMap<Integer, DupPublishMessageStore> map = stores.get(clientId);
        if (map == null || map.isEmpty()) {
            return Collections.emptyList();
        }
        return new ArrayList<>(map.values());
    }

    @Override
    public void remove(String clientId, int messageId) {
        ConcurrentHashMap<Integer, DupPublishMessageStore> map = stores.get(clientId);
        if (map != null) {
            map.remove(messageId);
            if (map.isEmpty()) {
                stores.remove(clientId, map);
            }
        }
        mirror(clientId);
    }

    @Override
    public void removeByClient(String clientId) {
        stores.remove(clientId);
        // 会话真正销毁: 内存与镜像一起清
        mirror(clientId);
    }

    @Override
    public void detach(String clientId) {
        // 只清内存, 镜像留给接管方节点加载
        stores.remove(clientId);
    }

    /**
     * 把当前快照交给持久镜像。
     *
     * <p>用 {@link ObjectProvider} 而不是直接注入: {@code InflightPersistence} 只在
     * 启用 Redis 时才存在, 且这样可以让本类与它之间不形成编译期的双向依赖。
     */
    private void mirror(String clientId) {
        if (persistence == null) {
            return;
        }
        InflightPersistence target = persistence.getIfAvailable();
        if (target == null) {
            return;
        }
        ConcurrentHashMap<Integer, DupPublishMessageStore> map = stores.get(clientId);
        target.onChanged(clientId, map == null ? List.of() : new ArrayList<>(map.values()));
    }

    @Override
    public int size() {
        int total = 0;
        for (ConcurrentHashMap<Integer, DupPublishMessageStore> map : stores.values()) {
            total += map.size();
        }
        return total;
    }
}
