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
package online.ipuff.jmqtt.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import online.ipuff.jmqtt.config.BrokerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.function.Supplier;

/**
 * Redis 连接的统一持有者与降级包装。
 *
 * <h2>为什么要有这一层</h2>
 * 所有使用 Redis 的组件都遵循同一套降级策略: 操作失败只记录并返回空结果, 不向上抛;
 * 状态变化时才打日志; 恢复后自动重新生效。把这段逻辑复制到每个组件里,
 * 迟早会有一处写得不一样 —— 而「有一处没降级」的后果就是 Redis 抖动时
 * 某个功能直接把异常抛到 MQTT 处理路径上, 拖垮整个接入。
 *
 * <h2>惰性连接</h2>
 * <b>刻意不在启动时连接</b>: Redis 挂掉不应该导致 broker 起不来。
 * 首次用到时再连, 失败就靠健康检查反复重试, 与
 * {@link #execute} 里的惰性重试共同保证恢复后自动生效。
 *
 * <h2>单连接多路复用</h2>
 * Lettuce 的连接是线程安全的, 命令会多路复用同一条 TCP。
 * 会话与队列相关的命令频率都不高, 不需要连接池 —— 池化只会引入额外的连接管理成本。
 *
 * <h2>健康状态的三态语义, 以及为什么不能用布尔值</h2>
 * 这里最初用的是一个从 {@code false} 起步的布尔标记, 结果踩了两个坑, 都不是理论问题:
 * <ol>
 *   <li><b>启动窗口内所有 Redis 功能被跳过。</b>端口在 Spring 容器就绪时就已监听,
 *       而健康检查的第一轮探活还要晚一点点 —— 这段时间内到达的 CONNECT
 *       会看到 {@code available=false}, 于是「恢复会话」被跳过、
 *       本该落盘的会话不落盘。设备在 broker 重启后会立刻重连, 撞上这个窗口的概率很高。</li>
 *   <li><b>一个功能的失败会熄掉所有功能。</b>标记是全局共享的: 在途镜像写入抛一次异常,
 *       同一时刻正在处理的 CONNECT 里的「取得归属」和「写入会话」就被一起跳过。
 *       两个独立功能之间出现了耦合 —— 这正是本文件反复强调要避免的那类问题。</li>
 * </ol>
 * 现在改成「<b>只知道故障、不假定健康</b>」: 初始为可用({@code downUntilMillis = 0}),
 * 失败后进入一段冷却期, 冷却结束自动重新尝试。拿到命令超时兜底的前提下,
 * 「先试一次」比「凭一个可能过期的标记直接放弃」要安全得多。
 *
 * <p><b>由此得到的调用约定:</b>
 * <ul>
 *   <li>{@link #available()} 回答的是「现在值不值得尝试」, <b>不是</b>「Redis 是否一定可用」。
 *       它只适合用在<b>可以安全跳过</b>的写路径上(跳过的代价只是这次不落盘)。</li>
 *   <li><b>恢复/读取路径不要用它做前置判断。</b>跳过一次恢复的代价是消息真的丢了,
 *       而直接尝试一次的代价只是一次命令超时。两者的代价不对称, 判断就不该对称。</li>
 * </ul>
 */
@Component
@ConditionalOnProperty(prefix = "jmqtt.broker.redis", name = "enabled", havingValue = "true")
public class RedisConnectionManager {

    private static final Logger log = LoggerFactory.getLogger(RedisConnectionManager.class);

    private final RedisClient redisClient;
    private final long healthIntervalMs;

    private volatile StatefulRedisConnection<String, String> connection;

    /** 已知故障的冷却截止时间(毫秒时间戳); 0 表示当前认为可用 */
    private volatile long downUntilMillis;

    public RedisConnectionManager(RedisClient redisClient, BrokerProperties properties) {
        this.redisClient = redisClient;
        this.healthIntervalMs = properties.redis().healthIntervalMs();
    }

    /**
     * 现在值不值得尝试 Redis。
     *
     * <p>注意语义: 返回 {@code true} 只代表「没有已知故障」, 不代表 Redis 一定可用。
     * 恢复/读取路径不应拿它当前置判断 —— 见类注释里的调用约定。
     */
    public boolean available() {
        long until = downUntilMillis;
        return until == 0L || System.currentTimeMillis() >= until;
    }

    /**
     * 取同步命令接口。连接不可达时抛出异常 —— 调用方应通过
     * {@link #execute} 使用, 而不是直接调用本方法。
     */
    public RedisCommands<String, String> commands() {
        return connection().sync();
    }

    /**
     * 统一的执行包装: 成功则清除故障标记, 失败则进入冷却期并只在状态<em>变化</em>时打日志。
     *
     * <p>失败一律返回 fallback, 不向上抛 —— 每一处调用点都在 MQTT 处理路径上,
     * 抛出去就意味着 Redis 抖动会直接变成连接/投递故障。
     *
     * <p>冷却期而不是永久降级: 失败后一段时间内让调用方的
     * {@link #available()} 判断为「不值得尝试」(避免对着一台挂掉的 Redis
     * 每个 CONNECT 都等一次命令超时), 冷却结束自动放行一次探测。
     * 不再依赖健康检查来「恢复」—— 任何一次成功都会立刻清除故障标记。
     */
    public <T> T execute(String op, Supplier<T> action, T fallback) {
        try {
            T result = action.get();
            if (downUntilMillis != 0L) {
                downUntilMillis = 0L;
                log.info("Redis 已恢复({})", op);
            }
            return result;
        } catch (Exception e) {
            boolean wasAvailable = available();
            downUntilMillis = System.currentTimeMillis() + healthIntervalMs;
            if (wasAvailable) {
                log.error("Redis 不可用, 依赖它的功能在 {}ms 内降级(本地投递不受影响)。op={}",
                        healthIntervalMs, op, e);
            } else {
                log.debug("Redis 仍在冷却期, op={}, err={}", op, e.toString());
            }
            return fallback;
        }
    }

    /**
     * 定期探活。既负责发现故障, 也负责发现恢复 —— 恢复后自动重新生效, 无需重启。
     */
    @Scheduled(fixedDelayString = "${jmqtt.broker.redis.health-interval-ms:5000}")
    public void healthCheck() {
        execute("ping", () -> commands().ping(), null);
    }

    /**
     * 惰性建立连接。失败时抛异常, 由调用方的 execute 包装成降级。
     */
    private StatefulRedisConnection<String, String> connection() {
        StatefulRedisConnection<String, String> current = connection;
        if (current != null && current.isOpen()) {
            return current;
        }
        synchronized (this) {
            if (connection != null && connection.isOpen()) {
                return connection;
            }
            connection = redisClient.connect();
            return connection;
        }
    }
}
