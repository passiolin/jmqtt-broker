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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import online.ipuff.jmqtt.config.BrokerProperties;
import online.ipuff.jmqtt.redis.RedisConnectionManager;
import online.ipuff.jmqtt.session.ConnectionRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 从 Redis 的命令队列取控制台下发的命令并执行。
 *
 * <h2>为什么用非阻塞轮询而不是 BRPOP</h2>
 * Blocking 版本看起来更优雅 —— 有命令立刻醒, 没命令零开销。但它有一个致命的副作用:
 * Lettuce 的多个命令<b>复用同一条 TCP 连接</b>, 而阻塞命令会把这条连接占住直到它返回。
 * 于是这个 poller 会在每次 BRPOP 期间, 把会话读写、在途镜像、离线队列全部堵在身后 ——
 * 一个「每 500 毫秒查一次管理命令」的组件, 变成了 MQTT 处理路径上的间歇性停顿。
 *
 * <p>轮询的代价是多几百毫秒的延迟。对一条人工触发、随后还会被持续观测进度的
 * 驱逐命令来说, 这个延迟完全无所谓; 而共享连接被占住是不可接受的。
 *
 * <h2>命令为什么会过期</h2>
 * 见 {@link AdminCommand#expiredAt(long)}: 队列里的命令没有 TCP 连接状态来保证时效,
 * 节点离线一小时后再把积压的驱逐命令执行掉, 比不执行危险得多。
 */
@Component
public class AdminCommandPoller {

    private static final Logger log = LoggerFactory.getLogger(AdminCommandPoller.class);

    /** 启动时若发现队列积压超过这个数就打一条警告 */
    private static final long BACKLOG_WARN_THRESHOLD = 100L;

    private final AdminProperties properties;
    private final AdminRedisKeys keys;
    private final ObjectProvider<RedisConnectionManager> redis;
    private final AdminCommandReporter reporter;
    private final AdminStatePublisher publisher;
    private final EvictionService evictionService;
    private final TopicCaptureService captureService;
    private final online.ipuff.jmqtt.metrics.NodeMetricsService nodeMetricsService;

    /** 只要为了「本节点当前连着多少客户端」填入命令结果 */
    private final ConnectionRegistry connectionRegistry;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private final String nodeId;
    private final AtomicBoolean startupBacklogChecked = new AtomicBoolean();

    public AdminCommandPoller(AdminProperties properties,
                              BrokerProperties brokerProperties,
                              AdminRedisKeys keys,
                              ObjectProvider<RedisConnectionManager> redis,
                              AdminCommandReporter reporter,
                              AdminStatePublisher publisher,
                              EvictionService evictionService,
                              ConnectionRegistry connectionRegistry,
                             TopicCaptureService captureService,
                             online.ipuff.jmqtt.metrics.NodeMetricsService nodeMetricsService) {
        this.properties = properties;
        this.nodeId = brokerProperties.id();
        this.keys = keys;
        this.redis = redis;
        this.reporter = reporter;
        this.publisher = publisher;
        this.evictionService = evictionService;
        this.captureService = captureService;
        this.connectionRegistry = connectionRegistry;
        this.nodeMetricsService = nodeMetricsService;
    }

    @Scheduled(fixedDelayString = "${jmqtt.broker.admin.command-poll-interval-ms:500}")
    public void poll() {
        if (!properties.enabled()) {
            return;
        }
        RedisConnectionManager manager = redis.getIfAvailable();
        if (manager == null) {
            return;
        }
        String queue = keys.commandQueue(nodeId);
        checkStartupBacklog(manager, queue);

        String payload = manager.execute("admin.command.poll",
                () -> manager.commands().rpop(queue), null);
        if (payload == null || payload.isBlank()) {
            return;
        }
        execute(payload);
    }

    private void execute(String payload) {
        AdminCommand command;
        try {
            command = objectMapper.readValue(payload, AdminCommand.class);
        } catch (Exception e) {
            // 解析失败就无法回写结果 —— 结果键是按命令 id 命名的, 而 id 就在这条坏数据里
            log.warn("管理命令解析失败, 已丢弃: {}", truncate(payload), e);
            return;
        }
        if (command.id() == null || command.type() == null) {
            log.warn("管理命令缺少 id 或 type, 已丢弃: {}", truncate(payload));
            return;
        }
        long now = System.currentTimeMillis();
        if (command.expiredAt(now)) {
            log.warn("管理命令已过期, 拒绝执行: id={} type={} 签发于 {}ms 前(上限 {}ms)",
                    command.id(), command.type(), now - command.issuedAt(), command.maxAgeMs());
            reporter.write(command.id(), EvictionState.EXPIRED.name(),
                    "命令已过期(签发于 " + (now - command.issuedAt()) + "ms 前), 未执行");
            return;
        }

        try {
            switch (command.type()) {
                case AdminCommand.TYPE_PING -> {
                    publisher.requestReconcile();
                    reporter.write(command.id(), result(command, EvictionState.COMPLETED, "pong"));
                }
                case AdminCommand.TYPE_SNAPSHOT -> {
                    publisher.requestReconcile();
                    reporter.write(command.id(), result(command, EvictionState.COMPLETED,
                            "已触发全量对账"));
                }
                case AdminCommand.TYPE_KICK -> {
                    boolean publishWill = command.spec() == null || command.spec().publishWillOrTrue();
                    boolean closed = evictionService.disconnect(command.clientId(), publishWill);
                    Map<String, String> fields = result(command, EvictionState.COMPLETED,
                            closed ? "已断开" : "该客户端不在本节点");
                    fields.put("clientId", String.valueOf(command.clientId()));
                    fields.put("found", String.valueOf(closed));
                    reporter.write(command.id(), fields);
                    log.info("踢下线: clientId={} 命中={} 发布遗嘱={}",
                            command.clientId(), closed, publishWill);
                }
                case AdminCommand.TYPE_EVICT -> {
                    // 结果由 EvictionService 自行持续上报(它会写进度), 这里不写
                    EvictionTask task = evictionService.submit(command.id(), command.spec(),
                            command.issuedAt());
                    if (task.state() == EvictionState.RECEIVED) {
                        log.info("驱逐命令已受理: id={} 计划={} 每批={} 间隔={}ms",
                                command.id(), task.target(), task.batchSize(), task.intervalMs());
                    }
                }
                case AdminCommand.TYPE_CAPTURE_START -> {
                    AdminCommand.CaptureSpec spec = command.capture();
                    if (spec == null) {
                        reporter.write(command.id(), result(command, EvictionState.REJECTED,
                                "缺少抓取参数"));
                    } else {
                        String error = captureService.start(spec.id(), spec.filter(), spec.clientId(),
                                spec.durationMinutes() == null ? 10 : spec.durationMinutes(),
                                spec.maxMessages() == null ? 1000 : spec.maxMessages());
                        reporter.write(command.id(), error == null
                                ? result(command, EvictionState.COMPLETED, "抓取已开始")
                                : result(command, EvictionState.REJECTED, error));
                    }
                }
                case AdminCommand.TYPE_CAPTURE_STOP -> {
                    captureService.stop(command.captureId());
                    reporter.write(command.id(), result(command, EvictionState.COMPLETED, "抓取已停止"));
                }
                case AdminCommand.TYPE_CLIENT_DETAIL -> {
                    String snapshot = publisher.clientSnapshotJson(command.clientId());
                    if (snapshot == null) {
                        reporter.write(command.id(), result(command, EvictionState.REJECTED,
                                "该客户端不在本节点(可能已断开或已迁移)"));
                    } else {
                        Map<String, String> fields = result(command, EvictionState.COMPLETED, "ok");
                        fields.put("snapshot", snapshot);
                        reporter.write(command.id(), fields);
                    }
                }
                case AdminCommand.TYPE_METRICS -> {
                    // 指标全部在内存里现成, 这里只做一次读取与序列化
                    Map<String, String> fields = result(command, EvictionState.COMPLETED, "ok");
                    fields.put("snapshot", nodeMetricsService.snapshotJson());
                    reporter.write(command.id(), fields);
                }
                case AdminCommand.TYPE_EVICT_ABORT -> {
                    boolean aborted = evictionService.abort(command.evictTaskId());
                    reporter.write(command.id(), result(command, EvictionState.COMPLETED,
                            aborted ? "已请求中止" : "没有正在执行的驱逐任务"));
                }
                default -> {
                    log.warn("未知的管理命令类型: {}", command.type());
                    reporter.write(command.id(), result(command, EvictionState.REJECTED,
                            "未知命令类型: " + command.type()));
                }
            }
        } catch (Exception e) {
            log.error("执行管理命令失败: id={} type={}", command.id(), command.type(), e);
            reporter.write(command.id(), result(command, EvictionState.FAILED, "执行出错: " + e));
        }
    }

    private Map<String, String> result(AdminCommand command, EvictionState state, String message) {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("taskId", command.id());
        fields.put("node", nodeId);
        fields.put("type", command.type());
        fields.put("state", state.name());
        fields.put("message", message);
        fields.put("issuedAt", String.valueOf(command.issuedAt()));
        fields.put("updatedAt", String.valueOf(System.currentTimeMillis()));
        fields.put("finishedAt", String.valueOf(System.currentTimeMillis()));
        fields.put("nodeConnections", String.valueOf(connectionRegistry.size()));
        return fields;
    }

    /**
     * 启动后第一次轮询时看一眼队列积压。
     *
     * <p>积压通常意味着节点离线期间控制台下发过命令。它们会在接下来的轮询里被逐条取出,
     * 并按年龄判定为过期 —— 这里只是把这个事实提前写进日志, 免得运维看到
     * 「节点刚起来就吐出一串过期命令」时不知所以。
     */
    private void checkStartupBacklog(RedisConnectionManager manager, String queue) {
        if (!startupBacklogChecked.compareAndSet(false, true)) {
            return;
        }
        Long length = manager.execute("admin.command.backlog",
                () -> manager.commands().llen(queue), null);
        if (length != null && length > BACKLOG_WARN_THRESHOLD) {
            log.warn("管理命令队列积压 {} 条(节点离线期间下发的命令), 将逐条取出并按年龄判定是否过期",
                    length);
        }
    }

    private static String truncate(String payload) {
        return payload.length() <= 200 ? payload : payload.substring(0, 200) + "...";
    }
}
