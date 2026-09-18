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

import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 连接注册表: clientId 与 Channel 的本地映射。
 *
 * <p><b>这里刻意只用一级查表。</b>「先查会话拿到连接标识, 再按标识查连接」这种两级设计
 * 看上去层次清晰, 实际把一条本可以 O(1) 的本地操作变成了:
 * <pre>
 *   SessionStore sessionStore = sessionStoreService.get(clientId);        // ① 可能是一次外部往返
 *   ChannelId channelId = channelIdMap.get(brokerId + "_" + channelId);   // ② 拼 key 再查
 *   Channel channel = channelGroup.find(channelId);                       // ③ 遍历
 * </pre>
 * 投递一条本地消息要付「1 次会话查询 + 2 次哈希查找 + 1 次全量遍历」, 而这发生在
 * <b>每条消息、每个订阅者</b>上。更麻烦的是中间那层标识里带 brokerId 是为「远程连接」准备的,
 * 但连接对象只存在于本 JVM —— 远程永远查不到, 这层间接性是纯粹的成本。
 *
 * <p>这里就是单层 {@link ConcurrentHashMap}: clientId -> Channel, 投递一次 get 结束。
 * 连接信息<b>只存在于进程内</b>, 不进任何外部存储。
 */
@Component
public class ConnectionRegistry {

    private static final Logger log = LoggerFactory.getLogger(ConnectionRegistry.class);

    private final ConcurrentHashMap<String, Channel> channels = new ConcurrentHashMap<>();

    /**
     * 全部连接。
     *
     * <p>这里用 {@link ChannelGroup} 只有一个用途: 优雅关闭时批量 close。
     * <b>不要用它做投递查找</b> —— 它的 {@code find()} 是全量遍历,
     * 投递路径上一旦出现遍历, 复杂度就随连接数线性增长。
     */
    private final ChannelGroup allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    private final AtomicInteger highWaterMarkCounter = new AtomicInteger();

    /**
     * 记录一条新连接(在 channelActive 时调用)。
     */
    public void addChannel(Channel channel) {
        allChannels.add(channel);
    }

    /**
     * 关闭全部连接。用于进程优雅退出, 让客户端感知断开并及时重连。
     */
    public void closeAll() {
        log.info("关闭全部连接, 当前数量: {}", allChannels.size());
        allChannels.close().awaitUninterruptibly();
        channels.clear();
    }

    /**
     * 注册连接。同一 clientId 重复注册时返回被替换掉的旧连接, 供调用方关闭(连接接管)。
     */
    public Channel register(String clientId, Channel channel) {
        Channel previous = channels.put(clientId, channel);
        if (previous != null && previous != channel) {
            log.info("clientId {} 发生连接接管, 旧连接将被关闭: {}", clientId, previous.id().asShortText());
        }
        return previous;
    }

    /**
     * 注销连接。
     *
     * <p>使用 {@code remove(key, value)} 语义: 只有当前注册的仍是这个 channel 时才移除。
     * 否则「旧连接断开」的延迟回调会误删刚刚重建的新连接 —— 这是重连场景下的典型竞态。
     */
    public boolean unregister(String clientId, Channel channel) {
        return channels.remove(clientId, channel);
    }

    public Channel get(String clientId) {
        return channels.get(clientId);
    }

    public boolean isOnline(String clientId) {
        Channel channel = channels.get(clientId);
        return channel != null && channel.isActive();
    }

    public Channel remove(String clientId) {
        return channels.remove(clientId);
    }

    public int size() {
        return channels.size();
    }

    /**
     * 全部在线连接。用于优雅关闭。
     */
    public Collection<Channel> channels() {
        return channels.values();
    }

    /**
     * 当前全部 clientId 的快照。<b>仅供管理面使用, 不要放进投递路径。</b>
     *
     * <p>每次调用都会复制一份键集合 —— 十万级连接下这是一次十万条的复制。
     * 管理面每秒最多查几次, 复制一次完全可以接受; 但它绝不能出现在
     * 「每条消息 × 每个订阅者」的路径上。
     *
     * <p>之所以要复制而不是直接暴露 {@code keySet()} 视图: 管理面的用法必然是
     * 「先拿全量候选, 再逐个断开」。而断开连接会修改这张表,
     * 一边遍历一边改会让 {@code ConcurrentHashMap} 的弱一致性迭代既可能漏也可能重 ——
     * 于是「计划断 N 条」就变成一个不准确的承诺。
     */
    public Set<String> clientIds() {
        return Set.copyOf(channels.keySet());
    }

    /**
     * 记录写缓冲高水位触发次数, 用于观测背压是否生效。
     */
    public int highWaterMarkCount() {
        return highWaterMarkCounter.get();
    }

    public void recordHighWaterMark() {
        highWaterMarkCounter.incrementAndGet();
    }
}
