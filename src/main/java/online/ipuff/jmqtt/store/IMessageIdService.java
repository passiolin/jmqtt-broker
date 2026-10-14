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

import java.util.function.IntPredicate;

/**
 * 报文标识符(Packet Identifier)生成器。
 */
public interface IMessageIdService {

    /**
     * 获取该会话的下一个报文标识符, 取值范围 1..65535。
     *
     * <p>唯一性只需在<b>单个会话的在途消息之间</b>保证(MQTT 3.1.1 §2.3.1)——
     * 计数器按会话隔离, 并跳过 {@code inUse} 命中的值(该会话已发出未确认的标识符)。
     * 在途数量受 {@code max-inflight} 约束(通常几十), 65535 的取值空间里
     * 绕回扫描总能找到空闲值。
     *
     * @param clientId 会话标识, 计数器按它隔离
     * @param inUse 判断某个标识符是否已被该会话的在途消息占用
     */
    int nextMessageId(String clientId, IntPredicate inUse);

    /**
     * 会话销毁时释放其计数器。
     */
    void release(String clientId);
}
