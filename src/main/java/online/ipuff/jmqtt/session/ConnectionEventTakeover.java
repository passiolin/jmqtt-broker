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
package online.ipuff.jmqtt.session;

import io.netty.channel.Channel;
import online.ipuff.jmqtt.cluster.ConnectionEvent;
import online.ipuff.jmqtt.cluster.TakeoverListener;
import online.ipuff.jmqtt.config.BrokerProperties;
import online.ipuff.jmqtt.handler.ChannelAttributes;
import online.ipuff.jmqtt.session.ISessionStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 跨节点接管的「触发侧」: 从连接事件流里发现「同一 clientId 已在别的节点上线」,
 * 释放本节点上的旧连接。
 *
 * <h2>为什么用连接事件, 而不是新增一条控制消息</h2>
 * 连接事件(connected / disconnected)本来就在发: 它带 {@code clientid}、
 * {@code node} 和 {@code connected_at}, 已经足够回答「谁在哪个节点、什么时候上线」。
 * 复用它的好处是<b>不新增记录类型</b>——数据面 JSON 契约不需要为控制指令开口子,
 * 而且对 {@code cleanSession=1} 同样有效(这类会话在 Redis 里没有归属记录,
 * 基于会话归属的通知覆盖不到)。
 *
 * <h2>迟到的事件绝不能踢掉新连接</h2>
 * 事件是异步的, 且每个节点都要读全量: 客户端从 B 切回本节点 A 之后,
 * A 仍可能收到 B 之前发出的 connected 事件。若不加判断就踢, 会把 A 上刚建立的
 * 连接打断, 客户端再次重连 —— 形成 flapping。
 *
 * <p>判据用事件里的 {@code connected_at} 与本地连接的接入时间:
 * <b>只有本地连接明显更老时才踢</b>。分不清(缺时间戳/差距小于时钟容差)就不动,
 * 交给 keepalive 超时兜底 —— 漏踢的代价是晚几十秒收敛, 误踢的代价是打断一条
 * 正常连接, 两者不对称。
 */
@Component
public class ConnectionEventTakeover {

    private static final Logger log = LoggerFactory.getLogger(ConnectionEventTakeover.class);

    /**
     * 节点间时钟偏差容差。同一机房 NTP 同步后通常在毫秒级,
     * 留 2s 是为了吸收虚拟化环境里的调度抖动; 它只会让「同时发生」的接管
     * 选择不踢(安全方向), 不会放大误踢。
     */
    static final long CLOCK_SKEW_TOLERANCE_MS = 2_000;

    private final BrokerProperties properties;
    private final ConnectionRegistry connectionRegistry;
    private final ISessionStoreService sessionStoreService;
    private final TakeoverListener takeoverListener;

    private final AtomicLong applied = new AtomicLong();
    private final AtomicLong skippedStale = new AtomicLong();
    private final AtomicLong skippedNotLocal = new AtomicLong();

    public ConnectionEventTakeover(BrokerProperties properties,
                                   ConnectionRegistry connectionRegistry,
                                   ISessionStoreService sessionStoreService,
                                   TakeoverListener takeoverListener) {
        this.properties = properties;
        this.connectionRegistry = connectionRegistry;
        this.sessionStoreService = sessionStoreService;
        this.takeoverListener = takeoverListener;
    }

    /**
     * 便宜的预过滤: 只凭 record key(clientId)判断本节点是否可能有关.
     *
     * <p>连接事件采用 {@code key=clientId}, 消费端不必先反序列化 JSON 就能拿到
     * clientId —— 绝大多数事件属于其他节点上的设备, 本节点既没有连接也没有会话,
     * 直接丢掉即可, 省掉一次 JSON 解析。
     *
     * <p>判据必须同时看「连接」和「会话」: 客户端已经离开本节点、但持久会话
     * 仍保留到过期, 这类残留副本正需要被接管清理。
     *
     * @return {@code false} 表示本节点与该 clientId 无关, 调用方无需解析事件体
     */
    public boolean shouldProcess(String clientId) {
        if (clientId == null || clientId.isBlank()) {
            return false;
        }
        if (connectionRegistry.get(clientId) != null) {
            return true;
        }
        if (sessionStoreService.get(clientId) != null) {
            return true;
        }
        skippedNotLocal.incrementAndGet();
        return false;
    }

    /**
     * 处理一条连接事件。
     *
     * @return {@code true} = 触发了本节点旧连接的释放
     */
    public boolean onEvent(ConnectionEvent event) {
        if (event == null || !ConnectionEvent.ACTION_CONNECTED.equals(event.action())) {
            return false;
        }
        String clientId = event.clientid();
        if (clientId == null || clientId.isBlank()) {
            return false;
        }
        // 本节点自己发出的事件: 本地接管已由 ConnectionRegistry.register 完成
        if (properties.id().equals(event.node())) {
            return false;
        }

        Channel local = connectionRegistry.get(clientId);
        if (local != null && local.isActive()) {
            Long localConnectedAt = local.attr(ChannelAttributes.CONNECTED_AT).get();
            Long remoteConnectedAt = event.connectedAt();
            if (localConnectedAt == null || remoteConnectedAt == null
                    || localConnectedAt + CLOCK_SKEW_TOLERANCE_MS >= remoteConnectedAt) {
                skippedStale.incrementAndGet();
                log.debug("忽略迟到的连接事件(本地连接更新或时间不可比): clientId={} "
                                + "本地接入={} 事件接入={} 事件节点={}",
                        clientId, localConnectedAt, remoteConnectedAt, event.node());
                return false;
            }
        }

        takeoverListener.onTakeover(clientId, event.node());
        applied.incrementAndGet();
        return true;
    }

    /** 因本类判断而释放的旧连接数。 */
    public long applied() {
        return applied.get();
    }

    /** 判定为迟到/不可比而放弃的事件数。该值持续增长说明事件延迟较高。 */
    public long skippedStale() {
        return skippedStale.get();
    }

    /** 与本节点无关(无连接、无会话)而在解析前丢弃的事件数。 */
    public long skippedNotLocal() {
        return skippedNotLocal.get();
    }
}
