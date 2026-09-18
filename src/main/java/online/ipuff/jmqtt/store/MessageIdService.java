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

import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 报文标识符生成器的进程内实现。
 *
 * <p>一种常见做法是把计数器放到共享存储上(如用 Redis 的 {@code INCR} 生成全局序号),
 * 目的是让多个 broker 实例之间不重复。但报文标识符的<b>唯一性只需在单个客户端会话内保证</b>
 * (MQTT 3.1.1 §2.3.1): 它标识的是「这条连接上的一次投递」, 跨实例共享没有意义,
 * 反而给每条 QoS 1/2 投递增加一次网络往返, 并让共享存储成为投递路径上的依赖。
 *
 * <p>这里用进程内 {@link AtomicInteger}, 取值范围 1..65535。
 */
@Service
public class MessageIdService implements IMessageIdService {

    private static final int MAX_MESSAGE_ID = 65535;

    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public int nextMessageId() {
        while (true) {
            int next = counter.updateAndGet(v -> v >= MAX_MESSAGE_ID ? 1 : v + 1);
            if (next > 0) {
                return next;
            }
        }
    }
}
