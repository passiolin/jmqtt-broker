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

/**
 * 控制台下发给节点的命令。
 *
 * <h2>为什么命令自带签发时间与最大年龄</h2>
 * 命令走的是「控制台写入 Redis 列表、节点取走执行」这条异步链路, 它没有 TCP 那样的
 * 连接状态来保证时效。于是出现一个必须处理的情况: 节点离线期间控制台下发了命令,
 * 一小时后节点起来了, 把这条命令取出来照常执行。
 *
 * <p>对「查看状态」这类命令没有影响, 但对「<b>驱逐</b>」来说后果严重 ——
 * 一小时后执行的驱逐, 打的是完全不同于当时的流量形态, 而且此时操作者多半已经
 * 忘了自己下发过它。<b>过期命令必须被拒绝, 而不是被补偿执行。</b>
 * 这也是这里不用 Pub/Sub 的原因: Pub/Sub 做不到「明确拒绝」, 它只能「收不到」。
 *
 * @param id          命令标识, 结果按它写入 {@code cmdr:{id}}
 * @param type        命令类型, 见本类的常量
 * @param issuedAt    控制台签发时间(epoch millis)
 * @param maxAgeMs    最大容忍年龄。超过则由节点拒绝并回 {@link EvictionState#EXPIRED}
 * @param clientId    {@link #TYPE_KICK} 的目标客户端
 * @param evictTaskId {@link #TYPE_EVICT_ABORT} 要中止的驱逐任务
 * @param spec        {@link #TYPE_EVICT} 的驱逐参数
 */
public record AdminCommand(
        String id,
        String type,
        long issuedAt,
        long maxAgeMs,
        String clientId,
        String evictTaskId,
        EvictionSpec spec,
        CaptureSpec capture,
        String captureId
) {

    /**
     * 消息监听任务参数({@link #TYPE_CAPTURE_START})。
     *
     * @param id              任务 id(由控制台生成, 即 Redis 键名的一部分)
     * @param filter          主题过滤器(MQTT 通配符)
     * @param durationMinutes 时长(分钟, 上限见 TopicCaptureService)
     * @param maxMessages     条数上限
     */
    public record CaptureSpec(
            String id,
            String filter,
            Integer durationMinutes,
            Integer maxMessages,
            String clientId
    ) {
    }

    /** 探活: 顺便让节点把最新概要写回 Redis */
    public static final String TYPE_PING = "PING";

    /** 踢掉单个客户端 */
    public static final String TYPE_KICK = "KICK";

    /** 提交一次批量驱逐 */
    public static final String TYPE_EVICT = "EVICT";

    /** 中止正在执行的驱逐 */
    public static final String TYPE_EVICT_ABORT = "EVICT_ABORT";

    /** 立即重建全量状态视图(客户端注册表 + 过滤器视图) */
    public static final String TYPE_SNAPSHOT = "SNAPSHOT";

    /** 开始一个消息监听任务(参数见 {@link CaptureSpec}) */
    public static final String TYPE_CAPTURE_START = "CAPTURE_START";

    /** 停止一个消息监听任务(数据保留给控制台删除/TTL) */
    public static final String TYPE_CAPTURE_STOP = "CAPTURE_STOP";

    /**
     * 查询某客户端<b>当下</b>的完整状态快照(含订阅列表)。
     * 控制台在人工点开客户端详情时按需下发 —— 订阅不随变化实时上报
     * (写放大不配这个低频动作), 快照在查询时刻现场构建。
     */
    public static final String TYPE_CLIENT_DETAIL = "CLIENT_DETAIL";

    /**
     * 查询本节点的运行时指标(当前累计值)。
     * broker 只维护最高效的当前状态, 历史序列由控制台定时采集并存 Redis。
     */
    public static final String TYPE_METRICS = "METRICS";

    /**
     * 是否已过期。
     *
     * <p>{@code maxAgeMs <= 0} 视为不设限 —— 便于在受控环境里手工调试。
     * 正常路径上控制台总会带上它。
     */
    public boolean expiredAt(long now) {
        return maxAgeMs > 0 && now - issuedAt > maxAgeMs;
    }
}
