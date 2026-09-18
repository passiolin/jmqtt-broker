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

import javax.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 管理面(管理控制台)配置。
 *
 * <h2>为什么单独开一份配置, 而不是塞进 BrokerProperties</h2>
 * 管理面是<b>可整体关掉</b>的一层: 它不参与任何 MQTT 报文的处理, 关掉之后 broker 的行为
 * 与之前完全一致。把它混进 broker 主配置, 会让「改一个展示项」和「改一条投递规则」
 * 看起来是一类操作 —— 而它们的风险等级差得很远。
 *
 * <h2>管理面为什么要走 Redis 而不是让控制台直连各节点</h2>
 * 控制台要的是「整个集群的视图」, 而它自己不在集群里。让它逐个 HTTP 请求各节点会带来两个问题:
 * 一是控制台必须能访问每个 broker 的管理端口(生产里这些端口通常不在同一张网里),
 * 二是「视图」这件事被拆成了 N 次 RPC, 任何一次失败都会让页面残缺。
 * 把状态发布到 Redis, 控制台只读一个地方 —— 节点是否可达由「心跳是否存在」表达,
 * 而不是由「请求是否超时」表达, 后者无法区分「节点挂了」和「网络抖动」。
 *
 * @param enabled                 管理面总开关。关掉后不再向 Redis 发布状态, 也不再轮询命令
 * @param heartbeatIntervalMs     心跳周期。同时决定集群视图的新鲜度与控制台判活的灵敏度
 * @param stateTtlSeconds         Redis 上状态键的存活时长。<b>必须显著大于心跳周期</b> ——
 *                                它是「节点已消失」的判定依据: 心跳停更后键自然过期,
 *                                控制台据此把该节点标为离线并忽略它的数据。
 *                                反过来, 若 TTL 太短, 一次 5 秒的 GC 停顿就会让节点被误判为下线
 * @param outboxCapacity          状态出站队列容量。队列满时丢弃并计数 ——
 *                                <b>绝不阻塞 MQTT 处理路径</b>, 丢掉的增量由后续的对账补回
 * @param filterPublishIntervalMs 订阅过滤器视图的重算间隔。订阅视图是从本地主题树重算出来的
 *                                (而不是靠增量计数), 因此不存在漂移; 这个间隔只决定新鲜度
 * @param maxFiltersPerClient     单个客户端最多在状态里带几个订阅过滤器。
 *                                它是 Redis 内存的护栏: 设备通常只有 1~3 个订阅,
 *                                但一个异常客户端可能订阅几千个过滤器, 若原样发布,
 *                                20 万个客户端就是几 GB
 * @param maxEvictPerTask         单次驱逐任务的硬上限。
 *                                <b>这是一条安全阀而不是性能参数</b>: 断连必然触发遗嘱发布,
 *                                一次性驱逐十万个客户端等于凭空制造十万条遗嘱风暴 ——
 *                                它会挤满路由、集群总线与保留存储, 而设备并没有真的下线。
 *                                把上限压在几千的量级, 等于强制「驱逐必须分批慢慢做」
 * @param defaultEvictBatchSize   未指定时的每批驱逐数量
 * @param minEvictIntervalMs      批次间隔下限, 防止把间隔设成 0 变成一次性全量断连
 * @param commandPollIntervalMs   命令队列轮询间隔。
 *                                <b>刻意用非阻塞轮询而不是 BRPOP</b>: Lettuce 的多条命令
 *                                复用同一条 TCP, 阻塞式命令会把这条连接占住,
 *                                连带把会话与在途镜像的读写一起卡住
 * @param commandMaxAgeMs         命令的最大容忍年龄。控制台破产或节点长时间离线时,
 *                                队列里可能积压过期命令 —— 一条「驱逐」命令若在一小时后
 *                                才被执行, 后果比不执行严重得多, 因此超龄命令一律拒绝
 * @param commandResultTtlSeconds 命令结果在 Redis 上的保留时长, 供控制台查询进度
 */
@ConfigurationProperties(prefix = "jmqtt.broker.admin")
public record AdminProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("5000") @Min(1000) int heartbeatIntervalMs,
        @DefaultValue("30") @Min(5) int stateTtlSeconds,
        @DefaultValue("200000") @Min(1000) int outboxCapacity,
        @DefaultValue("5000") @Min(500) int filterPublishIntervalMs,
        @DefaultValue("16") @Min(0) int maxFiltersPerClient,
        @DefaultValue("5000") @Min(1) int maxEvictPerTask,
        @DefaultValue("200") @Min(1) int defaultEvictBatchSize,
        @DefaultValue("50") @Min(20) int minEvictIntervalMs,
        @DefaultValue("500") @Min(50) int commandPollIntervalMs,
        @DefaultValue("60000") @Min(1000) long commandMaxAgeMs,
        @DefaultValue("3600") @Min(60) int commandResultTtlSeconds
) {

    /**
     * 状态键的实际 TTL(秒)。
     *
     * <p>取配置值与「3 个心跳周期」的较大者: 心跳 5 秒时 TTL 至少 15 秒,
     * 允许连续丢两次心跳而不被判死; 心跳调长时 TTL 跟着变长, 否则节点会被自己的配置饿死。
     */
    public int effectiveStateTtlSeconds() {
        long fromHeartbeat = (long) heartbeatIntervalMs / 1000 * 3;
        return (int) Math.max(stateTtlSeconds, Math.max(fromHeartbeat, 5));
    }
}
