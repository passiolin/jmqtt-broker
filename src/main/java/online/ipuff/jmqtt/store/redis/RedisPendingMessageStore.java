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
package online.ipuff.jmqtt.store.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.ScriptOutputType;
import online.ipuff.jmqtt.config.BrokerProperties;
import online.ipuff.jmqtt.redis.RedisConnectionManager;
import online.ipuff.jmqtt.store.IPendingMessageStore;
import online.ipuff.jmqtt.store.PendingMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 离线消息队列的 Redis 实现。
 *
 * <h2>键结构</h2>
 * <pre>
 *   {prefix}:queue:{{clientId}}    List  按入队顺序保存序列化后的待投递消息
 * </pre>
 *
 * <p>与 {@code session} / {@code subs} 用同一个 hash tag {@code {clientId}} ——
 * 入队脚本要顺带读会话的 expire 来设置 TTL, 两者必须在同一 slot,
 * 否则 Redis Cluster 会直接拒绝这条脚本。
 *
 * <h2>两个 Lua 脚本, 都是原子操作</h2>
 * <ul>
 *   <li><b>入队</b>: RPUSH → 超限则 LTRIM 丢最旧 → 按会话 expire 续 TTL, 一次往返完成。
 *       拆成多条命令的话, 并发入队会短暂突破上限, 且 TTL 可能漏设。</li>
 *   <li><b>取出</b>: LRANGE + LTRIM 必须原子, 否则两个并发取队列会拿到重复消息。</li>
 * </ul>
 *
 * <p>TTL 取自会话哈希的 {@code expire} 字段而不是每次多查一次 ——
 * 队列的存活期本来就该跟会话一致: 会话过期了, 欠它的消息也没有意义了。
 */
@Component
@ConditionalOnProperty(prefix = "jmqtt.broker.redis", name = "enabled", havingValue = "true")
public class RedisPendingMessageStore implements IPendingMessageStore {

    private static final Logger log = LoggerFactory.getLogger(RedisPendingMessageStore.class);

    private static final String KEY_SEGMENT_QUEUE = ":queue:";
    private static final String KEY_SEGMENT_SESSION = ":session:";

    /**
     * 入队 + 限长 + 续期。返回本次丢弃条数。
     *
     * <p>超限时丢最旧的: IoT 场景下「最近的数据」通常比「最早的数据」更有价值。
     */
    private static final String ENQUEUE_SCRIPT =
            "redis.call('RPUSH', KEYS[1], ARGV[1]) "
                    + "local len = redis.call('LLEN', KEYS[1]) "
                    + "local max = tonumber(ARGV[2]) "
                    + "local dropped = 0 "
                    + "if max > 0 and len > max then "
                    + "  redis.call('LTRIM', KEYS[1], len - max, -1) "
                    + "  dropped = len - max "
                    + "end "
                    + "local expire = redis.call('HGET', KEYS[2], 'expire') "
                    + "if expire then "
                    + "  local ttl = tonumber(expire) "
                    + "  if ttl and ttl > 0 then redis.call('EXPIRE', KEYS[1], ttl) end "
                    + "end "
                    + "return dropped";

    /**
     * 批量入队脚本: 一次 EVAL 携带同一客户端的多条消息(RPUSH 循环) → 超限 LTRIM
     * → 按会话 expire 续期。IO 次数从「每消息一次往返」降为「每客户端每轮一次」。
     */
    private static final String ENQUEUE_BATCH_SCRIPT =
            "local max = tonumber(ARGV[1]) "
                    + "local dropped = 0 "
                    + "for i = 2, #ARGV do "
                    + "  redis.call('RPUSH', KEYS[1], ARGV[i]) "
                    + "end "
                    + "local len = redis.call('LLEN', KEYS[1]) "
                    + "if max > 0 and len > max then "
                    + "  redis.call('LTRIM', KEYS[1], len - max, -1) "
                    + "  dropped = len - max "
                    + "end "
                    + "local expire = redis.call('HGET', KEYS[2], 'expire') "
                    + "if expire then "
                    + "  local ttl = tonumber(expire) "
                    + "  if ttl and ttl > 0 then redis.call('EXPIRE', KEYS[1], ttl) end "
                    + "end "
                    + "return dropped";

    /** 取出并移除前 limit 条。两条命令必须原子, 否则并发取队列会拿到重复消息。 */
    private static final String DRAIN_SCRIPT =
            "local limit = tonumber(ARGV[1]) "
                    + "if limit <= 0 then return {} end "
                    + "local items = redis.call('LRANGE', KEYS[1], 0, limit - 1) "
                    + "if #items > 0 then redis.call('LTRIM', KEYS[1], #items, -1) end "
                    + "return items";

    private final BrokerProperties properties;
    private final RedisConnectionManager redis;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final AtomicLong droppedCount = new AtomicLong();

    public RedisPendingMessageStore(BrokerProperties properties, RedisConnectionManager redis) {
        this.properties = properties;
        this.redis = redis;
        log.info("离线消息队列使用 Redis 实现, 单客户端上限 {} 条",
                properties.maxOfflineQueueLen());
    }

    @Override
    public boolean persistent() {
        return redis.available();
    }

    @Override
    public int enqueue(String clientId, PendingMessage message, int maxLen) {
        if (maxLen <= 0) {
            // 上限为 0 = 不缓存。这是明确的配置选择, 但仍要计数, 因为它意味着
            // cleanSession=0 客户端的 QoS 1/2 保证没有被兑现
            long total = droppedCount.incrementAndGet();
            if (total == 1 || total % 1000 == 0) {
                log.warn("离线消息被丢弃(上限为 0): clientId={} 累计 {} 条", clientId, total);
            }
            return 1;
        }
        String payload = encode(message);
        if (payload == null) {
            return 0;
        }
        // 必须是 Long: ScriptOutputType.INTEGER 解出来的是 Long, 写成 Integer 会抛
        // ClassCastException。副作用(入队)已经在服务端发生了, 只有返回值解码失败 ——
        // 结果是消息其实进了队列, 调用方却按「入队失败」处理, 属于最坏的一类不一致。
        Long dropped = redis.execute("queue.enqueue", () -> redis.commands().eval(
                ENQUEUE_SCRIPT,
                ScriptOutputType.INTEGER,
                new String[]{queueKey(clientId), sessionKey(clientId)},
                payload,
                Integer.toString(maxLen)), null);
        if (dropped == null) {
            // Redis 不可用: 消息没能入队。这比「入队后丢失」更糟, 必须让调用方知道
            redis.execute("queue.enqueueCounter", () -> {
                droppedCount.incrementAndGet();
                return null;
            }, null);
            log.warn("Redis 不可用, 离线消息无法缓存: clientId={} topic={}", clientId, message.topic());
            return 0;
        }
        if (dropped > 0) {
            long total = droppedCount.addAndGet(dropped);
            if (total == dropped || total % 1000 < dropped) {
                log.warn("离线队列超限, 丢弃最旧的 {} 条: clientId={} 上限 {} 累计丢弃 {} 条。"
                                + "这是 QoS 1/2 投递保证的实际破损点",
                        dropped, clientId, maxLen, total);
            }
        }
        return dropped.intValue();
    }

    /**
     * 批量入队: 同一客户端的多条消息合并为一次 EVAL。
     * 消失窗口与单条版一致: Redis 不可用时返回丢弃计数, 由调用方观察指标。
     */
    @Override
    public int enqueueBatch(String clientId, List<PendingMessage> messages, int maxLen) {
        if (messages == null || messages.isEmpty()) {
            return 0;
        }
        if (maxLen <= 0) {
            droppedCount.addAndGet(messages.size());
            return messages.size();
        }
        String[] argv = new String[messages.size() + 1];
        argv[0] = Integer.toString(maxLen);
        for (int i = 0; i < messages.size(); i++) {
            String payload = encode(messages.get(i));
            if (payload == null) {
                argv[i + 1] = "";
            } else {
                argv[i + 1] = payload;
            }
        }
        Long dropped = redis.execute("queue.enqueueBatch", () -> redis.commands().eval(
                ENQUEUE_BATCH_SCRIPT,
                ScriptOutputType.INTEGER,
                new String[]{queueKey(clientId), sessionKey(clientId)},
                argv), Long.valueOf(0));
        int d = dropped == null ? 0 : dropped.intValue();
        if (d > 0) {
            droppedCount.addAndGet(d);
        }
        return d;
    }

    @Override
    public List<PendingMessage> drainBatch(String clientId, int limit) {
        if (limit <= 0) {
            return List.of();
        }
        List<String> raw = redis.execute("queue.drain", () -> redis.commands().eval(
                DRAIN_SCRIPT,
                ScriptOutputType.MULTI,
                new String[]{queueKey(clientId)},
                Integer.toString(limit)), null);
        if (raw == null || raw.isEmpty()) {
            // Redis 不可用时返回空列表: 调用方会认为「没有更多积压」而结束投递,
            // 这正是期望行为 —— 下次重连会再试。抛出异常则会把故障扩散到连接建立路径。
            return List.of();
        }
        List<PendingMessage> messages = new ArrayList<>(raw.size());
        for (String item : raw) {
            PendingMessage message = decode(item);
            if (message != null) {
                messages.add(message);
            }
        }
        return messages;
    }

    @Override
    public int size(String clientId) {
        Long size = redis.execute("queue.size",
                () -> redis.commands().llen(queueKey(clientId)), null);
        return size == null ? 0 : size.intValue();
    }

    @Override
    public void removeAll(String clientId) {
        redis.execute("queue.removeAll", () -> redis.commands().del(queueKey(clientId)), null);
    }

    @Override
    public long droppedCount() {
        return droppedCount.get();
    }

    private String queueKey(String clientId) {
        return properties.redis().keyPrefix() + KEY_SEGMENT_QUEUE + "{" + clientId + "}";
    }

    private String sessionKey(String clientId) {
        return properties.redis().keyPrefix() + KEY_SEGMENT_SESSION + "{" + clientId + "}";
    }

    private String encode(PendingMessage message) {
        try {
            return objectMapper.writeValueAsString(new QueueItem(
                    message.topic(),
                    message.qos(),
                    message.payload() == null ? "" : Base64.getEncoder().encodeToString(message.payload()),
                    message.retain(),
                    message.createdAt()));
        } catch (Exception e) {
            log.warn("离线消息序列化失败, 丢弃该条: topic={}", message.topic(), e);
            return null;
        }
    }

    private PendingMessage decode(String json) {
        try {
            QueueItem item = objectMapper.readValue(json, QueueItem.class);
            byte[] payload = item.payload() == null || item.payload().isEmpty()
                    ? new byte[0]
                    : Base64.getDecoder().decode(item.payload());
            return new PendingMessage(item.topic(), item.qos(), payload, item.retain(), item.createdAt());
        } catch (Exception e) {
            log.warn("离线消息反序列化失败, 跳过该条", e);
            return null;
        }
    }

    /**
     * 队列中一条消息的持久化形态。
     *
     * <p>payload 用 base64 而不是塞进 header: 离线队列的量很小,
     * 可读性(能直接用 redis-cli 看内容)比那 33% 的空间更值。
     */
    private record QueueItem(String topic, int qos, String payload, boolean retain, long createdAt) {
    }
}
