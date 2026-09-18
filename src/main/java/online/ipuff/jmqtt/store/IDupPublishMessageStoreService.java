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

import java.util.List;

/**
 * QoS 1/2 未确认 PUBLISH 的存储服务(用于重连后重发 DUP=1)。
 *
 */
public interface IDupPublishMessageStoreService {

    void put(DupPublishMessageStore message);

    List<DupPublishMessageStore> get(String clientId);

    void remove(String clientId, int messageId);

    void removeByClient(String clientId);

    /**
     * 只清理本节点内存, <b>保留持久镜像</b>。
     *
     * <p>用于跨节点接管: 会话此刻已归属新节点, 由它加载这份镜像继续投递。
     * 用 {@link #removeByClient} 会把镜像一起删掉, 等于销毁新节点该投递的消息。
     */
    void detach(String clientId);

    int size();
}
