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
package online.ipuff.jmqtt.admin;

import online.ipuff.jmqtt.config.BrokerProperties;
import org.springframework.stereotype.Component;

/**
 * 管理面在 Redis 上的键约定。
 *
 * <h2>布局</h2>
 * <pre>
 * {prefix}:admin:nodes             SET    全部曾注册过的节点 id
 * {prefix}:admin:node:{nodeId}     HASH   心跳 + 节点概要, TTL = stateTtl
 * {prefix}:admin:clients:{nodeId}  HASH   clientId -> 紧凑 JSON(地址/版本/心跳/接入时间/订阅)
 * {prefix}:admin:filters:{nodeId}  HASH   topicFilter -> 订阅者数
 * {prefix}:admin:cmd:{nodeId}      LIST   控制台下发的命令(控制台 LPUSH, 节点 RPOP)
 * {prefix}:admin:cmdr:{cmdId}      HASH   命令结果与进度, TTL = commandResultTtl
 * </pre>
 *
 * <h2>三个关键设计点</h2>
 * <ol>
 *   <li><b>TTL 就是「节点还活着」的唯一依据。</b>节点每 {@code heartbeatIntervalMs}
 *       重写一次概要并续期三个状态键; 进程消失后键自然过期, 数据一并消失 ——
 *       不需要任何「死亡通知」机制, 也就不会出现「节点崩了但视图里还挂着一堆幽灵客户端」。</li>
 *   <li><b>命令用 LIST 而不是 Pub/Sub。</b>Pub/Sub 是「此刻在线的订阅者才收得到」,
 *       而节点可能在断线重连的瞬间错过命令, 且没有任何补偿 —— 对一条「驱逐」指令来说,
 *       「可能收不到」和「一定收到或明确过期」差别很大。LIST 有持久性,
 *       配合命令自带的签发时间与最大年龄, 能做到「要么按预期时间执行, 要么明确拒绝」。</li>
 *   <li><b>键按节点分片。</b>客户端注册表按 nodeId 分键, 而不是全集群一个 hash ——
 *       一是不同节点的生命周期本就独立(TTL 各自算), 二是避免所有节点的写入挤在同一个键上。</li>
 * </ol>
 */
@Component
public class AdminRedisKeys {

    private final String prefix;

    public AdminRedisKeys(BrokerProperties properties) {
        this.prefix = properties.redis().keyPrefix();
    }

    /** 节点 id 集合(长期存在, 用于发现曾出现过的节点) */
    public String nodes() {
        return prefix + ":admin:nodes";
    }

    /** 节点心跳与概要 */
    public String node(String nodeId) {
        return prefix + ":admin:node:" + nodeId;
    }

    /** 该节点的客户端注册表 */
    public String clients(String nodeId) {
        return prefix + ":admin:clients:" + nodeId;
    }

    /** 该节点的订阅过滤器视图 */
    public String filters(String nodeId) {
        return prefix + ":admin:filters:" + nodeId;
    }

    /** 该节点的命令队列 */
    public String commandQueue(String nodeId) {
        return prefix + ":admin:cmd:" + nodeId;
    }

    /** 命令结果与进度 */
    public String commandResult(String commandId) {
        return prefix + ":admin:cmdr:" + commandId;
    }
}
