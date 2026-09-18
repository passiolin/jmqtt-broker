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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package online.ipuff.jmqtt.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.ScriptOutputType;
import online.ipuff.jmqtt.config.BrokerProperties;
import online.ipuff.jmqtt.message.DupPublishMessageStore;
import online.ipuff.jmqtt.redis.RedisConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 在途消息（已发送未确认的 QoS 1/2）的持久镜像。
 *
 * <h2>它补的是哪个窗口</h2>
 * 在途消息原本只存在于进程内存。节点崩溃或发生跨节点接管时，
 * 这些「已经答应要投递给客户端」的消息就消失了 —— 客户端重连后拿不到，
 * QoS 1/2 的保证在节点故障场景下破损。
 *
 * <h2>为什么做成整份快照，而不是逐条操作日志</h2>
 * 每个客户端的在途集合有硬上限（{@code max-inflight}，通常几十条），
 * 所以「重写这一份」的成本是可接受的；而它带来两个关键好处：
 * <ol>
 *   <li><b>天然幂等、没有顺序问题。</b>逐条 HSET/HDEL 的日志在异步模式下必须保证顺序，
 *       否则一条迟到的 HSET 会把已经确认过的消息永久留在镜像里。
 *       整份覆盖是「最后写入者胜」，不存在这个问题。</li>
 *   <li><b>实现简单。</b>不需要维护操作队列、不需要回放。</li>
 * </ol>
 * {@link #onChanged} 只保留每个客户端<b>最新</b>的一份快照，未刷盘期间同一客户端
 * 连续变更会被自然合并（{@link ConcurrentHashMap#put} 覆盖）。
 *
 * <h2>三种模式，代价明确</h2>
 * <table>
 *   <tr><th>模式</th><th>行为</th><th>代价</th></tr>
 *   <tr><td>{@code off}</td><td>不落盘</td>
 *       <td>节点故障丢在途消息（原有行为）</td></tr>
 *   <tr><td>{@code sync}</td><td>每次变更同步写 Redis</td>
 *       <td><b>每条 QoS 1/2 消息多一次 Redis 往返, 直接抬高投递延迟</b></td></tr>
 *   <tr><td>{@code async}</td><td>标记脏客户端, 由单线程定时批量刷盘</td>
 *       <td>崩溃时丢最近一个刷盘周期的变更（默认 100ms）</td></tr>
 * </table>
 *
 * <p><b>选哪个取决于 QoS 1/2 的实际占比</b> ——
 * 若设备侧以 QoS 0 为主、只有下行指令用 QoS 1，同步写的开销可以忽略；
 * 若上行也用 QoS 1，就必须用 async 并接受那个窗口。
 * 占比可以从 {@code /open/api/jmqtt/info} 的 {@code qos} 段读出来。
 *
 * <p><b>异步模式用单线程刷盘</b>：不是为了让 Redis 操作串行（Lettuce 本身是异步的），
 * 而是为了让「提交」有序 —— 同一客户端的两份快照不会因为线程调度而倒序落盘。
 * 配合「只保留最新快照」的设计，即使偶尔乱序也只是短暂的不一致，下一次刷盘即纠正。
 *
 * <h2>为什么这个 Bean 不按 {@code redis.enabled} 条件装配</h2>
 * 它一开始是 {@code @ConditionalOnProperty(redis.enabled=true)} 的。后果是
 * {@code ConnectHandler} 对它形成了硬依赖, 而 {@code application.yml} 里
 * {@code redis.enabled} 默认就是 {@code false} —— <b>默认配置下整个应用起不来</b>,
 * 报 {@code NoSuchBeanDefinitionException}。Redis 是可选能力, 却把 broker 变成了必需。
 *
 * <p>根子在于用「Bean 存不存在」表达「功能开不开」: 条件 Bean 一旦被别人直接注入,
 * 就变成启动期的定时炸弹, 而且每个引用点都要自己决定是用 {@code ObjectProvider}
 * 还是硬注入 —— 同一个类里有两种写法, 迟早写错一处。
 * 现在改成<b>Bean 始终存在</b>, 用 {@link Mode#OFF} 表达「没启用 / 没有 Redis」。
 * 这样注入方式统一, 也不影响「Redis 挂了不拖垮接入」这条底线。
 */
@Component
public class InflightPersistence {

    private static final Logger log = LoggerFactory.getLogger(InflightPersistence.class);

    /** 运行模式 */
    public enum Mode {
        OFF,
        SYNC,
        ASYNC
    }

    private static final String KEY_SEGMENT_INFLIGHT = ":inflight:";
    private static final String KEY_SEGMENT_SESSION = ":session:";

    /**
     * 整份覆盖写入 + 按会话 expire 续期, 一次往返完成且原子。
     *
     * <p>用 DEL + HSET 而不是「HSET 新值 + HDEL 缺失值」: 后者需要记住上一次写了哪些字段,
     * 一旦刷盘失败就失去基准。DEL+HSET 在 Lua 里是原子的, 不存在中间空窗。
     */
    private static final String OVERWRITE_SCRIPT =
            "redis.call('DEL', KEYS[1]) "
                    + "if #ARGV > 1 then "
                    + "  local ttl = tonumber(ARGV[1]) "
                    + "  for i = 2, #ARGV, 2 do "
                    + "    redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1]) "
                    + "  end "
                    + "  if ttl and ttl > 0 then redis.call('EXPIRE', KEYS[1], ttl) end "
                    + "end "
                    + "return 1";

    /** 待刷盘的客户端 -> 最新快照。按 clientId 覆盖, 天然合并连续变更 */
    private final ConcurrentHashMap<String, List<DupPublishMessageStore>> pending = new ConcurrentHashMap<>();

    private final BrokerProperties properties;
    /** 未启用 Redis 时为 null —— 此时 {@link #mode()} 返回 {@link Mode#OFF} */
    private final RedisConnectionManager redis;
    private final Executor flushExecutor;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final AtomicLong flushCount = new AtomicLong();
    private final AtomicLong flushedEntries = new AtomicLong();
    private final AtomicLong failedCount = new AtomicLong();

    /**
     * 用 {@link ObjectProvider} 接收 Redis 连接管理器: 后者按
     * {@code jmqtt.broker.redis.enabled} 条件装配, 没有 Redis 时这里就是 null。
     *
     * <p>本类自己则<b>始终注册</b> —— 否则对它直接注入的组件会在禁用 Redis 时
     * 启动失败。见类注释。
     */
    public InflightPersistence(BrokerProperties properties,
                               ObjectProvider<RedisConnectionManager> redisProvider,
                               @Qualifier("inflightFlushExecutor") Executor flushExecutor) {
        this.properties = properties;
        this.redis = redisProvider.getIfAvailable();
        this.flushExecutor = flushExecutor;
        BrokerProperties.RedisProperties redisProperties = properties.redis();
        if (this.redis == null) {
            log.info("在途消息持久化未启用: 未开启 Redis(redis.enabled=false), 节点故障时在途消息会丢失");
        } else {
            log.info("在途消息持久化已启用: mode={} 刷盘周期={}ms",
                    redisProperties.inflightMode(), redisProperties.inflightFlushIntervalMs());
        }
    }

    /**
     * 当前模式。
     *
     * <p>没有 Redis 时直接是 {@link Mode#OFF} —— 配置文件里写的是 {@code sync}
     * 还是 {@code async} 都无所谓, 落盘本来就没有去处。
     */
    public Mode mode() {
        if (redis == null) {
            return Mode.OFF;
        }
        try {
            return Mode.valueOf(properties.redis().inflightMode().toUpperCase());
        } catch (Exception e) {
            log.warn("无法识别的 inflight-mode={}, 按 async 处理", properties.redis().inflightMode());
            return Mode.ASYNC;
        }
    }

    /**
     * 运行时是否值得往镜像里写。
     *
     * <p>只用于<b>写入</b>路径: 跳过一次写盘的代价是「崩在这个窗口里会丢在途消息」,
     * 而对着知道已经挂掉的 Redis 每个 QoS 1/2 投递都等一次命令超时,
     * 代价会直接落在投递热路径上。两端权衡之后, 写路径选择跳过。
     *
     * <p>读取/恢复路径不要用它 —— 见 {@link #load}。
     */
    public boolean enabled() {
        return mode() != Mode.OFF && redis.available();
    }

    // ------------------------------------------------------------------
    // 写入
    // ------------------------------------------------------------------

    /**
     * 某个客户端的在途集合发生变化。
     *
     * <p>调用方传入的是<b>变化后的完整快照</b> —— 传 null 或空列表表示该客户端已无在途消息,
     * 镜像会被删除。
     *
     * @param clientId 客户端
     * @param snapshot 变化后的完整在途集合
     */
    public void onChanged(String clientId, List<DupPublishMessageStore> snapshot) {
        if (!enabled() || clientId == null) {
            return;
        }
        List<DupPublishMessageStore> copy = (snapshot == null || snapshot.isEmpty())
                ? List.of()
                : List.copyOf(snapshot);

        if (mode() == Mode.SYNC) {
            persist(clientId, copy);
            return;
        }

        pending.put(clientId, copy);
        // 待刷盘客户端数超过阈值时立即提交一次 —— 避免未刷盘状态无限堆积。
        // 阈值取 maxSessions 的一个比例: 目的是「防止堆积」而不是「精确控制内存」。
        if (pending.size() >= properties.redis().inflightMaxPendingClients()) {
            submitFlush(pending.size());
        }
    }

    /**
     * 删除镜像。用于会话真正销毁（cleanSession=1 断开、会话过期）。
     *
     * <p><b>跨节点接管时不要调用</b> —— 会话此刻已归属新节点,
     * 镜像必须留给它加载。见 {@code IDupPublishMessageStoreService#detach}。
     */
    public void clear(String clientId) {
        if (clientId == null || mode() == Mode.OFF) {
            return;
        }
        pending.remove(clientId);
        redis.execute("inflight.clear", () -> redis.commands().del(inflightKey(clientId)), null);
    }

    // ------------------------------------------------------------------
    // 读取
    // ------------------------------------------------------------------

    /**
     * 加载某个客户端的在途镜像。客户端重连时调用。
     *
     * <p><b>刻意不判断 {@link #enabled()}。</b>这是恢复路径, 不是写入路径:
     * 因为一次 Redis 抖动就跳过加载, 等于把「已投递未确认」的 QoS 1/2 消息
     * 直接扔掉 —— 那正是本类存在的意义。读取失败时 {@code execute} 会返回
     * fallback, 代价上限是一次命令超时。
     *
     * @return 在途消息; 无记录或持久层不可用时返回空列表
     */
    public List<DupPublishMessageStore> load(String clientId) {
        if (clientId == null || mode() == Mode.OFF) {
            return List.of();
        }
        Map<String, String> fields = redis.execute("inflight.load",
                () -> redis.commands().hgetall(inflightKey(clientId)), null);
        if (fields == null || fields.isEmpty()) {
            return List.of();
        }
        List<DupPublishMessageStore> messages = new ArrayList<>(fields.size());
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            DupPublishMessageStore message = decode(clientId, entry.getKey(), entry.getValue());
            if (message != null) {
                messages.add(message);
            }
        }
        if (!messages.isEmpty()) {
            log.info("已从持久镜像恢复在途消息: clientId={} 条数={}", clientId, messages.size());
        }
        return messages;
    }

    // ------------------------------------------------------------------
    // 刷盘
    // ------------------------------------------------------------------

    /**
     * 周期性把待刷盘的快照写下去。仅 async 模式生效。
     */
    @Scheduled(fixedDelayString = "${jmqtt.broker.redis.inflight-flush-interval-ms:100}")
    public void flushDirty() {
        if (mode() != Mode.ASYNC || pending.isEmpty()) {
            return;
        }
        submitFlush(pending.size());
    }

    private void submitFlush(int observedSize) {
        for (Map.Entry<String, List<DupPublishMessageStore>> entry : pending.entrySet()) {
            String clientId = entry.getKey();
            List<DupPublishMessageStore> snapshot = pending.remove(clientId);
            if (snapshot == null) {
                continue;
            }
            try {
                flushExecutor.execute(() -> persist(clientId, snapshot));
            } catch (Exception e) {
                // 队列已满: 快照丢失, 但内存态仍然正确 —— 只影响崩溃后的可恢复性
                failedCount.incrementAndGet();
                log.warn("在途镜像刷盘任务提交失败, 该客户端的镜像暂时滞后: clientId={}", clientId, e);
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("提交在途镜像刷盘: 观察到的待刷盘客户端数={}", observedSize);
        }
    }

    private void persist(String clientId, List<DupPublishMessageStore> snapshot) {
        List<String> args = new ArrayList<>(1 + snapshot.size() * 2);
        args.add(Long.toString(sessionExpireSeconds(clientId)));
        for (DupPublishMessageStore message : snapshot) {
            String json = encode(message);
            if (json != null) {
                args.add(Integer.toString(message.messageId()));
                args.add(json);
            }
        }
        // 接收类型必须是 Long: Lettuce 的 ScriptOutputType.INTEGER 解码出的是 Long,
        // 声明成 Integer 会在运行期抛 ClassCastException。而诡异之处在于 ——
        // 脚本已经在服务端执行完了, 副作用(这里的 DEL+HSET)是真的发生了,
        // 只有返回值在解码时炸掉。表现就是「数据写进去了, 调用方却认为失败」。
        Long result = redis.execute("inflight.persist", () -> redis.commands().eval(
                OVERWRITE_SCRIPT,
                ScriptOutputType.INTEGER,
                new String[]{inflightKey(clientId), sessionKey(clientId)},
                args.toArray(new String[0])), null);
        if (result == null) {
            failedCount.incrementAndGet();
            return;
        }
        flushCount.incrementAndGet();
        flushedEntries.addAndGet(snapshot.size());
    }

    /**
     * 镜像的存活期跟会话一致 —— 会话过期了, 欠它的在途消息也没有意义了。
     */
    private long sessionExpireSeconds(String clientId) {
        String value = redis.execute("inflight.readExpire",
                () -> redis.commands().hget(sessionKey(clientId), "expire"), null);
        if (value == null) {
            return properties.sessionExpirySeconds();
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return properties.sessionExpirySeconds();
        }
    }

    private String inflightKey(String clientId) {
        // hash tag 让 inflight 与 session 落在同一 slot, Lua 才能同时操作两者
        return properties.redis().keyPrefix() + KEY_SEGMENT_INFLIGHT + "{" + clientId + "}";
    }

    private String sessionKey(String clientId) {
        return properties.redis().keyPrefix() + KEY_SEGMENT_SESSION + "{" + clientId + "}";
    }

    private String encode(DupPublishMessageStore message) {
        try {
            return objectMapper.writeValueAsString(new InflightItem(
                    message.topic(),
                    message.qos(),
                    message.payload() == null ? "" : Base64.getEncoder().encodeToString(message.payload())));
        } catch (Exception e) {
            log.warn("在途消息序列化失败, 跳过该条: topic={}", message.topic(), e);
            return null;
        }
    }

    /**
     * 解码一条在途消息。
     *
     * <p>报文标识符取自哈希的<b>字段名</b>, 而不是 value 里的某个字段 ——
     * 字段名本来就是 packetId, 重复存一份只会带来两处不一致的可能。
     */
    private DupPublishMessageStore decode(String clientId, String field, String json) {
        try {
            int messageId = Integer.parseInt(field);
            InflightItem item = objectMapper.readValue(json, InflightItem.class);
            byte[] payload = item.payload() == null || item.payload().isEmpty()
                    ? new byte[0]
                    : Base64.getDecoder().decode(item.payload());
            return new DupPublishMessageStore(clientId, item.topic(), item.qos(), messageId, payload);
        } catch (Exception e) {
            log.warn("在途消息反序列化失败, 跳过该条: field={}", field, e);
            return null;
        }
    }

    // ------------------------------------------------------------------
    // 指标
    // ------------------------------------------------------------------

    public Map<String, Long> stats() {
        return Map.of(
                "inflightFlushCount", flushCount.get(),
                "inflightFlushedEntries", flushedEntries.get(),
                "inflightPendingClients", (long) pending.size(),
                "inflightFlushFailed", failedCount.get());
    }

    private record InflightItem(String topic, int qos, String payload) {
    }
}
