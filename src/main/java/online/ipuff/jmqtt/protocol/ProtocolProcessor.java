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
package online.ipuff.jmqtt.protocol;

import org.springframework.stereotype.Component;

/**
 * 协议处理器聚合。
 *
 * <p>这些处理器都是无状态单例, 直接构造器注入即可。
 * 不需要为它们写懒加载 —— 手写 {@code volatile} 字段加双重检查锁, 本质上是在
 * 容器之外又实现了一小半容器, 而那一半不会有依赖检查、不会有循环依赖检测。
 */
@Component
public class ProtocolProcessor {

    private final ConnectHandler connectHandler;
    private final PublishHandler publishHandler;
    private final SubscribeHandler subscribeHandler;
    private final UnsubscribeHandler unsubscribeHandler;
    private final PubAckHandler pubAckHandler;
    private final PubRecHandler pubRecHandler;
    private final PubRelHandler pubRelHandler;
    private final PubCompHandler pubCompHandler;
    private final PingReqHandler pingReqHandler;
    private final DisconnectHandler disconnectHandler;

    public ProtocolProcessor(ConnectHandler connectHandler,
                             PublishHandler publishHandler,
                             SubscribeHandler subscribeHandler,
                             UnsubscribeHandler unsubscribeHandler,
                             PubAckHandler pubAckHandler,
                             PubRecHandler pubRecHandler,
                             PubRelHandler pubRelHandler,
                             PubCompHandler pubCompHandler,
                             PingReqHandler pingReqHandler,
                             DisconnectHandler disconnectHandler) {
        this.connectHandler = connectHandler;
        this.publishHandler = publishHandler;
        this.subscribeHandler = subscribeHandler;
        this.unsubscribeHandler = unsubscribeHandler;
        this.pubAckHandler = pubAckHandler;
        this.pubRecHandler = pubRecHandler;
        this.pubRelHandler = pubRelHandler;
        this.pubCompHandler = pubCompHandler;
        this.pingReqHandler = pingReqHandler;
        this.disconnectHandler = disconnectHandler;
    }

    public ConnectHandler connect() {
        return connectHandler;
    }

    public PublishHandler publish() {
        return publishHandler;
    }

    public SubscribeHandler subscribe() {
        return subscribeHandler;
    }

    public UnsubscribeHandler unsubscribe() {
        return unsubscribeHandler;
    }

    public PubAckHandler pubAck() {
        return pubAckHandler;
    }

    public PubRecHandler pubRec() {
        return pubRecHandler;
    }

    public PubRelHandler pubRel() {
        return pubRelHandler;
    }

    public PubCompHandler pubComp() {
        return pubCompHandler;
    }

    public PingReqHandler pingReq() {
        return pingReqHandler;
    }

    public DisconnectHandler disconnect() {
        return disconnectHandler;
    }
}
