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

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 跨节点会话恢复验证。用法: SessionRestoreCheck <节点A端口> <节点B端口>
 *
 * <p>两个节点共享同一 Redis。验证的核心不变量:
 * <b>回 sessionPresent=true 时, 订阅必须同时被恢复。</b>
 * 只恢复会话不恢复订阅, 客户端会以为订阅仍有效而消息永远到不了 ——
 * 这是最难排查的静默故障。
 */
public class SessionRestoreCheck {

    static final String HOST = "127.0.0.1";

    record Pub(String topic, String body) {
    }

    /** CONNACK 结果: sessionPresent 是本次验证的核心观测值 */
    record ConnAck(boolean sessionPresent) {
    }

    static final BlockingQueue<ConnAck> connAcks = new LinkedBlockingQueue<>();
    static final BlockingQueue<Pub> inbox = new LinkedBlockingQueue<>();

    static int passed = 0;
    static int failed = 0;

    public static void main(String[] args) throws Exception {
        int portA = Integer.parseInt(args[0]);
        int portB = Integer.parseInt(args[1]);
        EventLoopGroup group = new NioEventLoopGroup(2);
        try {
            System.out.println("=== 跨节点会话恢复验证 (节点A:" + portA + ", 节点B:" + portB + ") ===");

            // ---- 1. 在节点 A 建立 cleanSession=0 会话并订阅 ----
            Channel client = connect(group, portA, "sess-restore", false);
            ConnAck first = awaitConnAck();
            check("新会话在节点 A 上 sessionPresent=false",
                    first != null && !first.sessionPresent(), "实际=" + (first == null ? "无响应" : first.sessionPresent()));

            client.writeAndFlush(subscribe(1, "restore/+/data", MqttQoS.AT_LEAST_ONCE));
            Thread.sleep(800);
            ok("节点 A 订阅 restore/+/data 完成");

            // ---- 2. 正常断开(cleanSession=0 应保留会话与订阅) ----
            client.writeAndFlush(new MqttMessage(
                    new MqttFixedHeader(MqttMessageType.DISCONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0)));
            Thread.sleep(1200);
            ok("已发送 DISCONNECT(cleanSession=0, 会话应保留)");

            // ---- 3. 重连到节点 B —— 关键观测点 ----
            Channel reconnected = connect(group, portB, "sess-restore", false);
            ConnAck second = awaitConnAck();
            check("重连到另一个节点 B 时 sessionPresent=true(会话从持久层恢复)",
                    second != null && second.sessionPresent(),
                    "实际=" + (second == null ? "无响应" : second.sessionPresent()));

            // ---- 4. 订阅是否真的恢复了: 在节点 B 上发布并观察投递 ----
            Thread.sleep(500);
            inbox.clear();
            Channel publisher = connect(group, portB, "sess-pub", true);
            awaitConnAck();
            Thread.sleep(400);

            publisher.writeAndFlush(publish("restore/room1/data", 20, "restored-subscription"));
            Pub got = inbox.poll(8, TimeUnit.SECONDS);
            check("订阅已被恢复: 节点 B 发布后客户端收到消息",
                    got != null && "restored-subscription".equals(got.body()),
                    "实际=" + (got == null ? "未收到" : got.topic() + " body=" + got.body()));

            // ---- 5. cleanSession=1 不应产生持久化会话 ----
            Channel clean = connect(group, portB, "sess-clean", true);
            ConnAck cleanAck = awaitConnAck();
            check("cleanSession=1 的新会话 sessionPresent=false",
                    cleanAck != null && !cleanAck.sessionPresent(),
                    "实际=" + (cleanAck == null ? "无响应" : cleanAck.sessionPresent()));

            clean.writeAndFlush(subscribe(2, "restore/clean/data", MqttQoS.AT_LEAST_ONCE));
            Thread.sleep(600);
            clean.writeAndFlush(new MqttMessage(
                    new MqttFixedHeader(MqttMessageType.DISCONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0)));
            Thread.sleep(800);

            Channel cleanAgain = connect(group, portB, "sess-clean", true);
            ConnAck cleanAck2 = awaitConnAck();
            check("cleanSession=1 断开后重连仍为 sessionPresent=false(未持久化)",
                    cleanAck2 != null && !cleanAck2.sessionPresent(),
                    "实际=" + (cleanAck2 == null ? "无响应" : cleanAck2.sessionPresent()));

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
        return connAcks.poll(10, TimeUnit.SECONDS);
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
