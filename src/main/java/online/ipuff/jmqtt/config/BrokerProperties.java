/**
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
package online.ipuff.jmqtt.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Broker 配置。
 *
 * <p>采用 Spring Boot 的 {@code @ConfigurationProperties} <b>构造器绑定</b>而不是
 * setter 注入: 配置对象因此可以是不可变 record, 字段上的约束注解在绑定期就会生效,
 * 写错的配置在启动时直接失败, 而不是等到第一次用到才发现。
 */
@Validated
@ConfigurationProperties(prefix = "jmqtt.broker")
public record BrokerProperties(

        /** Broker 唯一标识, 集群模式下用于实例判断 */
        @NotBlank String id,

        /** 监听地址, 留空表示绑定所有网卡 */
        String host,

        @Min(1) int port,

        boolean websocketEnabled,
        @Min(1) int websocketPort,
        @NotBlank String websocketPath,

        @DefaultValue("0") @Min(0) int bossThreads,
        @Min(0) int workerThreads,
        boolean useEpoll,
        /** 每秒新建连接上限, 0 表示不限速。超速的连接在 event loop 上排队等待, 不拒绝。 */
        @DefaultValue("4096") @Min(0) int connectRatePerSecond,

        boolean authEnabled,
        String authUsername,
        String authPassword,

        @Min(0) int defaultKeepAlive,
        @Min(0) long sessionExpirySeconds,
        /**
         * v5: 服务端可接受的<b>入站</b> topic alias 数量上限。
         *
         * <p>本实现不做入站别名(别名需要为每条连接维护一张 alias→topic 映射,
         * 还要处理「未知别名」「空主题名 + 别名」这些错误路径), 因此默认为 0,
         * 并在 CONNACK 里如实声明 —— 客户端看到 0 就不会使用别名。
         * 比「声明支持然后悄悄忽略」安全得多。
         */
        @Min(0) int topicAliasMaximum,
        /**
         * 服务端强制的心跳间隔(秒)。0 表示不干预, 沿用客户端协商值。
         *
         * <p>置为非 0 时会在 CONNACK 里回 {@code Server Keep Alive} 属性,
         * 客户端<b>必须</b>改用该值发送心跳。用途是把心跳下限握在服务端手里 ——
         * 否则一个客户端声明 keepAlive=600 就能让死连接占着资源十分钟。
         */
        @Min(0) int serverKeepAlive,
        @Min(1) int maxInflight,
        @Min(1) int maxMqueueLen,
        /**
         * 每个持久会话最多缓存多少条离线消息。超出后<b>丢弃最旧的</b>。
         *
         * <p>必须有上限: 一个长期离线的客户端会无限累积消息, 最终把内存或 Redis 撑爆。
         * 选择丢最旧而非最新, 是因为 IoT 场景下「最近的数据」通常比「最早的数据」更有价值。
         * 丢弃会被计数并打日志 —— 它是 QoS 1/2 保证的实际破损点, 不能静默发生。
         */
        @Min(0) int maxOfflineQueueLen,

        boolean clusterEnabled,
        @NotNull KafkaProperties kafka,
        @NotNull RedisProperties redis,

        @Min(1) int soBacklog,
        boolean soKeepAlive,
        boolean tcpNoDelay,
        @Min(64) int maxPayloadSize,
        @Min(0) int writeBufferLowWaterMark,
        @Min(0) int writeBufferHighWaterMark
) {

    /**
     * boss 线程数, 0 表示与 CPU 核数相同。
     * 注意: 单端口监听下实际只有 1 个 loop 承接 accept, 多出的线程平时空闲 ——
     * 多端口(TCP+WebSocket)或高频接入场景下按核数提供余量。
     */
    public int bossThreadsOrDefault() {
        return bossThreads > 0 ? bossThreads : Runtime.getRuntime().availableProcessors();
    }

    /**
     * worker 线程数, 0 表示 2 * CPU 核数。
     */
    public int workerThreadsOrDefault() {
        return workerThreads > 0 ? workerThreads : Runtime.getRuntime().availableProcessors() * 2;
    }

    /**
     * 每秒新建连接上限。0 表示不限速。
     */
    public int connectRatePerSecondOrDefault() {
        return connectRatePerSecond > 0 ? connectRatePerSecond : Integer.MAX_VALUE;
    }

    /**
     * 上行数据出口(Kafka)配置。
     *
     * <p>这里把「消息面」与「数据面」拆成两条链路, 因为两者的流量特征完全不同:
     * <ul>
     *   <li><b>消息面</b>（{@code clusterTopic}）: 跨节点 pub/sub, 需要每个节点都读到
     *       —— 每个节点使用<b>各自独立的 group.id</b>, 即 Kafka 官方语义下的广播</li>
     *   <li><b>数据面</b>（{@code uplinkTopic}）: 上行遥测, 只需下游消费一次,
     *       不需要每个 broker 都读</li>
     * </ul>
     * 混在一条链路上会让数据面白白承受 N 倍扇出。
     *
     * @param enabled          Kafka 总开关。集群能力依赖它, 关闭时装配单机总线
     * @param bootstrapServers broker 地址, 逗号分隔
     * @param clientIdPrefix   客户端 ID 前缀
     * @param clusterTopic     跨节点广播 topic。Kafka record 的 <b>key 是 MQTT 主题</b>,
     *                         既保证同一主题落在同一 partition(维持顺序),
     *                         也是消费端还原主题的唯一途径
     * @param groupId          消费组 ID。<b>每个节点必须不同</b> —— 相同则变成组内分摊(队列语义),
     *                         不同才会广播到所有节点。留空时自动生成为 {@code prefix-brokerId}
     * @param broadcastEnabled 消息面(集群广播)总开关。<b>只有「其他节点上的 MQTT 订阅者也需要这条
     *                         消息」时才需要广播。</b>如果业务上只有设备与服务端通信(设备与其他端之间
     *                         走 P2P 信令, 本来就有成本更低、时效更好的方案), 那么让每个节点消费
     *                         全量消息就纯粹是成本。关掉后本节点投递与数据面上行都不受影响,
     *                         唯一的后果是其他节点上的 MQTT 订阅者收不到本节点发布的消息
     * @param broadcastFilters 只有匹配这些过滤器的主题才进集群广播。留空表示全部主题都广播。
     *                         用于混合场景: 例如只留 {@code app/+/cmd} 与 {@code device/+/signal},
     *                         设备上报的海量遥测就不必让 N 个节点各读一遍
     * @param uplinkTopic      上行数据 topic。留空表示不开启数据面出口
     * @param uplinkFilters    哪些主题算「上行」。为空表示全部消息都视为上行
     * @param uplinkExclusive  命中上行过滤器的消息是否<b>只</b>走数据面、不再进集群广播。
     *                         当上行消费者是服务端(直连 Kafka, 而非 MQTT 订阅)时应当置 true,
     *                         可把扇出从 N 倍降到 1 倍
     * @param uplinkKey        数据面的分区键:<br>
     *                         {@code topic}(默认) —— 键 = MQTT 主题, 保证同一主题的消息有序;<br>
     *                         {@code device} —— 键 = 设备标识(clientId), 保证<b>每个设备自己的
     *                         时序</b>有序, 并按设备把流量散到各分区。<br>
     *                         <b>遥测类主题通常高度集中(甚至只有一个), 此时必须用 device</b> ——
     *                         按主题分区会把全部流量压进一个分区, 下游加再多消费者也只能串行读。
     *                         设备标识缺失时(如服务端主动下发)自动回退为主题。
     * @param maxBlockMs       生产者缓冲或元数据不可用时的最长阻塞。<b>必须设小</b>,
     *                         否则 Kafka 故障会直接卡住 MQTT 发布路径
     * @param queueCapacity    出站队列容量。队列满时丢弃并计数 ——
     *                         宁可丢跨节点消息, 也不能阻塞本节点投递
     * @param consumerThreads  集群消费并行度(worker 数)。记录按<b>分区</b>哈希到固定
     *                         worker —— 由于广播 record 的 key 恒为 MQTT 主题(同主题必落
     *                         同分区), 同主题的消息仍严格有序, 并行只发生在不同分区之间。
     *                         缺省 0(= 2 × CPU 核数); 消费跟不上时调大,
     *                         有效上限是 topic 的分区数。
     *                         <b>同时是下行通道(downlink)worker 池的大小</b> ——
     *                         容量压测证明单线程下行在逐条回显的对称流量下 ≈5.5k msg/s 封顶,
     *                         下行已改为与消息面同构的按分区并行消费
     * @param compressionType  压缩算法, 如 lz4 / snappy / zstd / none
     * @param autoOffsetReset  无已提交 offset 时的起点, latest 或 earliest
     * @param connectionEventTopic 连接事件(上线/下线)发布的专用 topic。
     *                         留空(默认)关闭; 非空时 record 的 key=clientId
     *                         (同一客户端的事件严格有序), value 为 JSON。
     *                         供后台系统消费设备生命周期, 不参与集群消息面
     * @param routes           多路由上行: 每条路由一组过滤器 + 目标 topic + 分区键,
     *                         一条消息命中多条路由就各发一份。配置了 routes 时
     *                         以它为准(旧的 uplink-topic 单路由配置被忽略);
     *                         未命中任何路由的消息不发 Kafka, 只走集群广播
     * @param downlink         下行通道: broker 消费这些 topic 并按本地订阅投递为
     *                         MQTT 消息(每节点独立 group, 只做本地投递, 绝不回写
     *                         集群或上行 —— 结构性防回环)。留空(默认)关闭
     */
    public record KafkaProperties(
            boolean enabled,
            String bootstrapServers,
            String clientIdPrefix,
            String clusterTopic,
            String groupId,
            boolean broadcastEnabled,
            List<String> broadcastFilters,
            String uplinkTopic,
            List<String> uplinkFilters,
            boolean uplinkExclusive,
            @Min(0) int maxBlockMs,
            @Min(1) int queueCapacity,
            @Min(1) int pollTimeoutMs,
            @DefaultValue("0") @Min(0) int consumerThreads,
            String compressionType,
            String autoOffsetReset,
            String uplinkKey,
            String connectionEventTopic,
            List<Route> routes,
            List<Downlink> downlink
    ) {

        /**
         * 上行路由: 过滤器命中的消息发到 {@code topic}, 分区键按 {@code key}
         * ({@code topic} 或 {@code device}, 语义同 {@code uplink-key})。
         */
        public record Route(
                List<String> filters,
                @NotBlank String topic,
                @DefaultValue("topic") String key
        ) {

            public boolean keyByDevice() {
                return "device".equalsIgnoreCase(key);
            }
        }

        /**
         * 下行通道: broker 消费 {@code topic} 并按本地订阅投递。
         * 投递 QoS 以消息体的 qos 字段为准(缺省 1), 通道不再配置。
         */
        public record Downlink(
                @NotBlank String topic
        ) {
        }


        /**
         * 该节点实际使用的消费组 ID。
         *
         * <p>留空时按 {@code clientIdPrefix-brokerId} 生成 —— 保证每节点独立。
         */
        public String resolvedGroupId(String brokerId) {
            if (groupId != null && !groupId.isBlank()) {
                return groupId;
            }
            String prefix = (clientIdPrefix == null || clientIdPrefix.isBlank()) ? "jmqtt" : clientIdPrefix;
            return prefix + "-" + brokerId;
        }

        public boolean uplinkEnabled() {
            return uplinkTopic != null && !uplinkTopic.isBlank();
        }

        /**
         * 集群消费 worker 数。0(缺省) = 2 × CPU 核数。
         * 消息面与下行通道各自有一套同构的 worker 池, 都取这个大小。
         */
        public int consumerThreadsOrDefault() {
            return consumerThreads > 0 ? consumerThreads
                    : Runtime.getRuntime().availableProcessors() * 2;
        }

        /**
         * 集群广播是否覆盖全部主题(即未配置过滤器)。
         *
         * <p>「未配置过滤器」的语义是<b>全部广播</b>, 而不是「什么都不广播」——
         * 后者会让漏配一个字段变成「静默丢掉所有跨节点消息」。
         */
        public boolean broadcastAll() {
            return broadcastFilters == null || broadcastFilters.isEmpty();
        }

        /**
         * 数据面是否按设备标识分区。
         *
         * <p>默认按主题分区(与集群广播一致), 需要按设备时序读遥测时改成 {@code device}。
         */
        public boolean uplinkKeyByDevice() {
            return "device".equalsIgnoreCase(uplinkKey);
        }

        /**
         * 这条上行消息实际使用的分区键。
         *
         * <p>设备标识缺失时回退为主题 —— 例如服务端主动下发的消息没有来源客户端。
         * 回退而不是发 null key: null key 会让 Kafka 在各分区之间轮转,
         * 于是同一个设备的消息会散到不同分区, 有序性直接消失。
         */
        public String uplinkPartitionKey(String clientId, String topic) {
            if (uplinkKeyByDevice() && clientId != null && !clientId.isBlank()) {
                return clientId;
            }
            return topic;
        }
    }

    /**
     * 会话持久化(Redis)配置。
     *
     * <p>只用于会话的<b>可恢复性</b> —— 存会话属性与订阅关系, 让客户端重连到
     * 任意节点都能恢复会话。三条必须遵守的约束:
     * <ol>
     *   <li><b>会话记录必须包含订阅关系。</b>MQTT 规范中订阅是 session state 的一部分;
     *       只存会话属性不存订阅, 恢复后只能回 {@code sessionPresent=false},
     *       否则客户端会以为订阅仍有效而静默丢消息。</li>
     *   <li><b>只在连接 / 重连 / 接管 / 订阅变更这类低频路径上读写。</b>
     *       绝不进入消息投递路径 —— 那会退化成「每次投递一次 Redis 往返」。</li>
     *   <li><b>Redis 不可用时降级为新会话</b>({@code sessionPresent=false}),
     *       接入必须保持可用。丢会话的代价是一轮重新订阅, 拒绝接入的代价是设备全部掉线。</li>
     * </ol>
     *
     * @param enabled      总开关。关闭时装配空实现, 会话仅存在于进程内
     * @param host         Redis 主机
     * @param port         Redis 端口
     * @param password     密码, 可为空
     * @param database     库号
     * @param keyPrefix    键前缀
     * @param commandTimeoutMs 单条命令超时。<b>必须设小</b> —— 这些命令都在
     *                     连接建立路径上, Redis 卡住会直接拖慢握手
     * @param healthIntervalMs 健康检查间隔。决定 Redis 故障与恢复被感知的时延
     * @param inflightMode 在途消息(已发未确认的 QoS 1/2)的持久化模式:
     *        {@code off}(不落盘) / {@code sync}(每次变更同步写) /
     *        {@code async}(定时批量刷盘, 默认)。
     *        <b>sync 的代价是每条 QoS 1/2 消息多一次 Redis 往返</b>, 会直接抬高投递延迟;
     *        async 的代价是崩溃时丢最近一个刷盘周期的变更。选哪个取决于 QoS 1/2 的实际占比。
     * @param inflightFlushIntervalMs async 模式的刷盘周期, 也就是该模式下的丢失窗口
     * @param inflightMaxPendingClients 待刷盘客户端数超过该值时立即触发一次刷盘,
     *        防止未刷盘状态无限堆积
     * @param mode              部署模式: {@code standalone}(默认, 单机)/
     *        {@code sentinel}(哨兵, 主从自动故障转移)/{@code cluster}(分片集群)。
     *        本项目只存会话元数据, 通常单机或哨兵即可; 集群模式下
     *        {@code database} 必须为 0(集群只有 db0)
     * @param masterId          哨兵模式的主节点名称(sentinel monitor 的名字), 仅 sentinel 需要
     * @param nodes             哨兵/集群的节点地址列表(host:port)。sentinel 模式列出
     *        哨兵进程地址; cluster 模式列出集群种子节点(任一可达即可)
     */
    public record RedisProperties(
            boolean enabled,
            String host,
            @Min(1) int port,
            String password,
            @Min(0) int database,
            String keyPrefix,
            @Min(100) int commandTimeoutMs,
            @Min(1000) int healthIntervalMs,
            String inflightMode,
            @Min(10) int inflightFlushIntervalMs,
            @Min(1) int inflightMaxPendingClients,
            @DefaultValue("standalone") String mode,
            String masterId,
            List<String> nodes
    ) {

        public boolean sentinelMode() {
            return "sentinel".equalsIgnoreCase(mode);
        }

        public boolean clusterMode() {
            return "cluster".equalsIgnoreCase(mode);
        }
    }
}
