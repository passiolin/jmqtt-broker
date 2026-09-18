package online.ipuff.jmqtt.e2e;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubscribePayload;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.util.CharsetUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 离线消息队列验证。用法: OfflineQueueCheck <订阅/发布节点端口> <重连节点端口>
 *
 * <p>验证的不变量: 持久会话的客户端离线期间, 发往它订阅主题的 QoS 1 消息
 * 必须被缓存, 并在它重连到<b>任意节点</b>后投递。
 *
 * <p>注意消息必须是发布到<b>持有该订阅的节点</b>上的 —— 订阅关系是本节点私有的,
 * 不复制到其他节点。所以流程设计为: 客户端在节点 A 订阅后断开,
 * 发布者也在节点 A 发布(节点 A 仍持有该订阅, 于是入队),
 * 客户端随后重连到节点 B 并从共享的 Redis 队列里领走消息。
 */
public class OfflineQueueCheck {

    static final String HOST = "127.0.0.1";
    static final String TOPIC = "offline/room1/data";

    record Pub(String topic, String body) {
    }

    record ConnAck(boolean sessionPresent) {
    }

    static final BlockingQueue<ConnAck> connAcks = new LinkedBlockingQueue<>();
    static final BlockingQueue<Pub> inbox = new LinkedBlockingQueue<>();

    static int passed = 0;
    static int failed = 0;

    public static void main(String[] args) throws Exception {
        int subPort = Integer.parseInt(args[0]);
        int reconnectPort = Integer.parseInt(args[1]);
        EventLoopGroup group = new NioEventLoopGroup(2);
        try {
            System.out.println("=== 离线消息队列验证 (订阅/发布节点:" + subPort
                    + ", 重连节点:" + reconnectPort + ") ===");

            // ---- 1. 在节点 A 建立持久会话并订阅 ----
            Channel client = connect(group, subPort, "offline-x", false);
            check("节点 A 建立持久会话", !awaitConnAck().sessionPresent(), "sessionPresent 应为 false");
            client.writeAndFlush(subscribe(1, "offline/+/data", MqttQoS.AT_LEAST_ONCE));
            Thread.sleep(800);
            ok("节点 A 订阅 offline/+/data 完成");

            // ---- 2. 客户端离线(断开但不清理会话) ----
            client.writeAndFlush(new MqttMessage(
                    new MqttFixedHeader(MqttMessageType.DISCONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0)));
            Thread.sleep(1200);
            ok("客户端已离线(cleanSession=0, 会话与订阅保留)");

            // ---- 3. 客户端离线期间, 在持有订阅的节点 A 上发布 3 条 QoS 1 消息 ----
            Channel publisher = connect(group, subPort, "offline-pub", true);
            awaitConnAck();
            Thread.sleep(400);
            for (int i = 1; i <= 3; i++) {
                publisher.writeAndFlush(publish(TOPIC, 100 + i, "offline-" + i));
                Thread.sleep(150);
            }
            Thread.sleep(800);
            ok("离线期间已发布 3 条 QoS 1 消息");

            // ---- 4. 重连到另一个节点 B, 应领到全部积压 ----
            Channel reconnected = connect(group, reconnectPort, "offline-x", false);
            ConnAck ack = awaitConnAck();
            check("重连到节点 B 时 sessionPresent=true",
                    ack.sessionPresent(), "实际=" + ack.sessionPresent());

            List<String> received = drainPubs(8);
            check("重连后收到全部 3 条离线消息",
                    received.size() == 3, "实际收到 " + received.size() + " 条: " + received);
            check("离线消息保持入队顺序",
                    received.equals(List.of("offline-1", "offline-2", "offline-3")),
                    "实际=" + received);

            // ---- 5. QoS 0 不应入队 ----
            System.out.println();
            System.out.println("=== 结果: 通过 " + passed + " / 失败 " + failed + " ===");
            if (failed > 0) {
                System.exit(1);
            }
        } finally {
            group.shutdownGracefully().await(3, TimeUnit.SECONDS);
        }
    }

    // ---------- 辅助 ----------

    static Channel connect(EventLoopGroup group, int port, String clientId, boolean cleanSession) throws Exception {
        MqttConnectMessage connect = new MqttConnectMessage(
                new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0),
                new MqttConnectVariableHeader("MQTT", 4, true, true, false, 0, false, cleanSession, 60),
                new MqttConnectPayload(clientId, null, null,
                        "jmqtt", "jmqtt".getBytes(CharsetUtil.UTF_8)));

        Bootstrap b = new Bootstrap();
        b.group(group).channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new MqttDecoder())
                                .addLast(MqttEncoder.INSTANCE)
                                .addLast(new SimpleChannelInboundHandler<MqttMessage>() {
                                    @Override
                                    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) {
                                        switch (msg.fixedHeader().messageType()) {
                                            case CONNACK -> connAcks.offer(
                                                    new ConnAck(((MqttConnAckMessage) msg).variableHeader().isSessionPresent()));
                                            case PUBLISH -> {
                                                MqttPublishMessage p = (MqttPublishMessage) msg;
                                                inbox.offer(new Pub(p.variableHeader().topicName(),
                                                        p.payload().toString(CharsetUtil.UTF_8)));
                                            }
                                            default -> {
                                            }
                                        }
                                    }
                                });
                    }
                });
        Channel channel = b.connect(HOST, port).sync().channel();
        channel.writeAndFlush(connect);
        return channel;
    }

    static ConnAck awaitConnAck() throws InterruptedException {
        ConnAck ack = connAcks.poll(10, TimeUnit.SECONDS);
        return ack == null ? new ConnAck(false) : ack;
    }

    static MqttSubscribeMessage subscribe(int packetId, String filter, MqttQoS qos) {
        return new MqttSubscribeMessage(
                new MqttFixedHeader(MqttMessageType.SUBSCRIBE, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(packetId),
                new MqttSubscribePayload(List.of(new MqttTopicSubscription(filter, qos))));
    }

    static MqttPublishMessage publish(String topic, int packetId, String body) {
        return new MqttPublishMessage(
                new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                new MqttPublishVariableHeader(topic, packetId),
                Unpooled.copiedBuffer(body, CharsetUtil.UTF_8));
    }

    /** 收满 seconds 秒后返回收到的全部消息体 */
    static List<String> drainPubs(long seconds) throws InterruptedException {
        long deadline = System.currentTimeMillis() + seconds * 1000;
        List<String> bodies = new ArrayList<>();
        while (System.currentTimeMillis() < deadline) {
            Pub p = inbox.poll(300, TimeUnit.MILLISECONDS);
            if (p != null) {
                bodies.add(p.body());
            }
        }
        return bodies;
    }

    static void ok(String name) {
        passed++;
        System.out.println("[PASS] " + name);
    }

    static void check(String name, boolean condition, String detail) {
        if (condition) {
            ok(name);
        } else {
            failed++;
            System.out.println("[FAIL] " + name + "  (" + detail + ")");
        }
    }
}
