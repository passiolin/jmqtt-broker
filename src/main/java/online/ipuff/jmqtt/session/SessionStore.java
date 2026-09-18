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

import online.ipuff.jmqtt.message.WillMessage;

/**
 * 会话状态。
 *
 * <p><b>两个有意为之的取舍:</b>
 * <ul>
 *   <li><b>会话里不存连接标识。</b>连接是进程内易失信息, 会话是跨进程需持久的信息。
 *       把两者放在同一对象里, 每次重连都要重写整份会话 —— 而重连恰恰是最频繁的操作之一。
 *       连接映射由 {@link ConnectionRegistry} 单独承担。</li>
 *   <li><b>心跳超时与会话过期不共用字段。</b>用 {@code keepAlive * 1.5} 兼作会话 TTL
 *       看起来能省一个配置, 实际会让「客户端心跳停了」直接等价于「会话可以删了」——
 *       于是 {@code cleanSession=0} 的会话在客户端短暂离线后被静默清掉,
 *       表现是设备重连后 {@code sessionPresent=false}、订阅和离线消息全部丢失,
 *       而且没有任何报错。这两件事的时间尺度本来就该独立配置。</li>
 * </ul>
 */
public class SessionStore {

    /** 归属 broker 标识 */
    private final String brokerId;

    private final String clientId;

    /**
     * CONNECT 时客户端要求「丢弃既有会话」。
     *
     * <p>v3.1.1 里叫 {@code cleanSession}, v5 里叫 {@code cleanStart} —— 语义相同。
     * <b>注意它只描述「本次连接怎么开始」, 不描述「断开后留不留」</b>:
     * v5 允许 {@code cleanStart=1} 且 Session Expiry Interval &gt; 0,
     * 即从零开始建立、但建立之后要保留。所以判断「要不要保留」一律用
     * {@link #isPersistent()}, 不要用这个字段。
     */
    private final boolean cleanSession;

    /**
     * 会话保留时长(秒); 0 表示不保留。
     *
     * <p>不是 final: v5 允许客户端在 DISCONNECT 时改写它（只能减小），
     * 所以连接建立之后它仍可能变化。
     */
    private volatile long expireSeconds;

    /** 会话创建时间(epoch millis) */
    private final long createdAt;

    /** 遗嘱消息, 可为 null */
    private volatile WillMessage willMessage;

    /** 最近一次活跃时间(epoch millis), 用于过期判定 */
    private volatile long lastActiveAt;

    public SessionStore(String brokerId, String clientId, boolean cleanSession, long expireSeconds) {
        this(brokerId, clientId, cleanSession, expireSeconds, null, System.currentTimeMillis());
    }

    public SessionStore(String brokerId, String clientId, boolean cleanSession, long expireSeconds,
                        WillMessage willMessage, long createdAt) {
        this.brokerId = brokerId;
        this.clientId = clientId;
        this.cleanSession = cleanSession;
        this.expireSeconds = expireSeconds;
        this.willMessage = willMessage;
        this.createdAt = createdAt;
        this.lastActiveAt = createdAt;
    }

    public boolean isExpired() {
        if (expireSeconds <= 0) {
            return false;
        }
        return System.currentTimeMillis() - lastActiveAt > expireSeconds * 1000L;
    }

    public void touch() {
        this.lastActiveAt = System.currentTimeMillis();
    }

    public String getBrokerId() {
        return brokerId;
    }

    public String getClientId() {
        return clientId;
    }

    public boolean isCleanSession() {
        return cleanSession;
    }

    /**
     * 会话是否需要保留 —— 断开后继续存在、要落盘、要接收离线消息。
     *
     * <p><b>判据只有一个: 保留时长 &gt; 0。</b>这样一个判断同时覆盖两个版本的语义:
     * <ul>
     *   <li>v3.1.1: {@code cleanSession=1} 被翻译成保留时长 0, {@code =0} 被翻译成配置的默认时长</li>
     *   <li>v5: Session Expiry Interval 直接就是保留时长, 0 即「断开即销毁」</li>
     * </ul>
     * 之所以不能用 {@code !cleanSession} 代劳: v5 存在
     * {@code cleanStart=1 && SessionExpiryInterval>0} 这种组合 ——
     * 会话从零开始, 但建立之后要按 SEI 保留。用 cleanStart 判会把它误判成「不保留」。
     */
    public boolean isPersistent() {
        return expireSeconds > 0;
    }

    public long getExpireSeconds() {
        return expireSeconds;
    }

    /**
     * 改写会话保留时长。仅用于 v5 的 DISCONNECT 覆盖, 调用方负责校验只能减小。
     */
    public void setExpireSeconds(long expireSeconds) {
        this.expireSeconds = expireSeconds;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public long getLastActiveAt() {
        return lastActiveAt;
    }

    public WillMessage getWillMessage() {
        return willMessage;
    }

    public SessionStore setWillMessage(WillMessage willMessage) {
        this.willMessage = willMessage;
        return this;
    }
}
