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

import online.ipuff.jmqtt.message.DupPubRelMessageStore;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * QoS 2 未完成 PUBREL 的进程内存储。
 *
 */
@Service
public class DupPubRelMessageStoreService implements IDupPubRelMessageStoreService {

    /** clientId -> (messageId -> PUBREL) */
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, DupPubRelMessageStore>> stores =
            new ConcurrentHashMap<>();

    @Override
    public void put(DupPubRelMessageStore message) {
        stores.computeIfAbsent(message.clientId(), k -> new ConcurrentHashMap<>())
                .put(message.messageId(), message);
    }

    @Override
    public List<DupPubRelMessageStore> get(String clientId) {
        ConcurrentHashMap<Integer, DupPubRelMessageStore> map = stores.get(clientId);
        if (map == null || map.isEmpty()) {
            return Collections.emptyList();
        }
        return new ArrayList<>(map.values());
    }

    @Override
    public void remove(String clientId, int messageId) {
        ConcurrentHashMap<Integer, DupPubRelMessageStore> map = stores.get(clientId);
        if (map != null) {
            map.remove(messageId);
            if (map.isEmpty()) {
                stores.remove(clientId, map);
            }
        }
    }

    @Override
    public void removeByClient(String clientId) {
        stores.remove(clientId);
    }

    @Override
    public int size() {
        int total = 0;
        for (ConcurrentHashMap<Integer, DupPubRelMessageStore> map : stores.values()) {
            total += map.size();
        }
        return total;
    }
}
