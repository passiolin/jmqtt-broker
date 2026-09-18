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
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubscribePayload;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.util.CharsetUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * jmqtt-broker 端到端验证。需要 broker 已在本机 1883 端口运行。
 */
public class MqttE2ECheck {

    static final String HOST = "127.0.0.1";
    static final int PORT = 1883;

    /** 报文快照: 必须在 handler 内取值, 否则 ByteBuf 会被 Netty 释放 */
    record Pub(String topic, boolean retain, boolean dup, int qos, String body) {
    }

    static final BlockingQueue<Pub> inbox = new LinkedBlockingQueue<>();
    static final BlockingQueue<MqttConnAckMessage> connAcks = new LinkedBlockingQueue<>();
    static final BlockingQueue<MqttSubAckMessage> subAcks = new LinkedBlockingQueue<>();
    static final CountDownLatch connAckLatch = new CountDownLatch(3);
    static final CountDownLatch subAckLatch = new CountDownLatch(2);

    static int passed = 0;
    static int failed = 0;

    public static void main(String[] args) throws Exception {
        EventLoopGroup group = new NioEventLoopGroup(2);
        try {
            System.out.println("=== jmqtt-broker 端到端验证 ===");

            // ---- 1. CONNECT ----
            Channel sub1 = connect(group, "e2e-sub-1", true, null, null);
            Channel pub1 = connect(group, "e2e-pub-1", true, null, null);
            Channel sub2 = connect(group, "e2e-sub-2", false, null, null);
            if (!connAckLatch.await(10, TimeUnit.SECONDS)) {
                fail("CONNACK 未在 10s 内全部收到");
                return;
            }
            ok("3 个客户端 CONNECT 成功");

            // ---- 2. sessionPresent ----
            /// sub2 使用 cleanSession=0 且服务端无该会话。
            /// 按 !cleanSession 直接推断 sessionPresent 的写法在这里会错误地返回 true。
            List<MqttConnAckMessage> ackList = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                MqttConnAckMessage a = connAcks.poll(2, TimeUnit.SECONDS);
                if (a != null) {
                    ackList.add(a);
                }
            }
            check("全部 3 个客户端收到 CONNACK", ackList.size() == 3, "数量=" + ackList.size());
            boolean anyPresent = ackList.stream().anyMatch(a -> a.variableHeader().isSessionPresent());
            check("全新会话的 sessionPresent 全部为 false (含 cleanSession=0)", !anyPresent, "存在 true");

            Thread.sleep(200);

            // ---- 3. SUBSCRIBE 通配符 ----
            sub1.writeAndFlush(subscribe(1, "sensor/+/temp", MqttQoS.AT_LEAST_ONCE));
            // ---- 4. 含非法过滤器的 SUBSCRIBE: 应逐项返回 0x80, 不断连 ----
            sub2.writeAndFlush(subscribeMixed(2, "sensor/room2/temp", "sport/tennis#"));

            if (!subAckLatch.await(10, TimeUnit.SECONDS)) {
                fail("SUBACK 未在 10s 内全部收到");
                return;
            }
            List<List<Integer>> grantedLists = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                MqttSubAckMessage a = subAcks.poll(2, TimeUnit.SECONDS);
                if (a != null) {
                    grantedLists.add(a.payload().grantedQoSLevels());
                }
            }
            check("通配符订阅 sensor/+/temp 返回 QoS 1", grantedLists.contains(List.of(1)),
                    "实际=" + grantedLists);
            check("非法过滤器 sport/tennis# 逐项返回 0x80, 合法项放行",
                    grantedLists.contains(List.of(1, 0x80)), "实际=" + grantedLists);
            check("含非法过滤器后连接保持存活", sub2.isActive(), "isActive=" + sub2.isActive());

            Thread.sleep(200);

            // ---- 5. PUBLISH QoS1 经通配符匹配投递 ----
            inbox.clear();
            pub1.writeAndFlush(publish("sensor/room1/temp", 100, "23.5", MqttQoS.AT_LEAST_ONCE, false));
            Pub delivered = waitPub(8);
            check("通配符订阅收到 sensor/room1/temp",
                    delivered != null && "sensor/room1/temp".equals(delivered.topic())
                            && "23.5".equals(delivered.body()),
                    "实际=" + describe(delivered));

            // ---- 6. 不命中的主题不应投递 ----
            inbox.clear();
            pub1.writeAndFlush(publish("sensor/room1/humidity", 101, "60", MqttQoS.AT_LEAST_ONCE, false));
            check("不匹配 sensor/+/temp 的主题未被投递", waitPub(2) == null, "出现预期外的投递");

            // ---- 7. retain: 保留消息对晚到的订阅者补投 ----
            pub1.writeAndFlush(publish("sensor/room2/temp", 102, "30.1", MqttQoS.AT_LEAST_ONCE, true));
            Thread.sleep(600);
            Channel sub3 = connect(group, "e2e-sub-3", true, null, null);
            Thread.sleep(500);
            inbox.clear();
            sub3.writeAndFlush(subscribe(9, "sensor/room2/temp", MqttQoS.AT_LEAST_ONCE));
            Pub retained = waitPub(8);
            check("晚到的订阅者收到保留消息且 retain=1",
                    retained != null && retained.retain() && "30.1".equals(retained.body()),
                    "实际=" + describe(retained));

            // ---- 8. 遗嘱: 异常断连触发 ----
            Channel willClient = connect(group, "e2e-will-1", true, "sensor/will/topic", "device-died");
            Thread.sleep(500);
            sub1.writeAndFlush(subscribe(20, "sensor/will/topic", MqttQoS.AT_MOST_ONCE));
            Thread.sleep(400);
            inbox.clear();
            willClient.close();
            Thread.sleep(500);
            Pub will = waitPub(8);
            check("异常断连触发遗嘱消息",
                    will != null && "device-died".equals(will.body()), "实际=" + describe(will));

            // ---- 9. 正常 DISCONNECT 不应触发遗嘱 ----
            inbox.clear();
            Channel willClient2 = connect(group, "e2e-will-2", true, "sensor/will/topic", "should-not-fire");
            Thread.sleep(500);
            willClient2.writeAndFlush(new MqttMessage(
                    new MqttFixedHeader(MqttMessageType.DISCONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0)));
            Thread.sleep(800);
            check("正常 DISCONNECT 不触发遗嘱", waitPub(3) == null, "遗嘱被错误触发");

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

    static Channel connect(EventLoopGroup group, String clientId, boolean cleanSession,
                           String willTopic, String willPayload) throws Exception {
        boolean hasWill = willTopic != null;
        MqttConnectVariableHeader variableHeader = new MqttConnectVariableHeader(
                "MQTT", 4, true, true, false, 0, hasWill, cleanSession, 60);
        MqttConnectPayload payload = new MqttConnectPayload(
                clientId, willTopic, hasWill ? willPayload.getBytes(CharsetUtil.UTF_8) : null,
                "jmqtt", "jmqtt".getBytes(CharsetUtil.UTF_8));
        MqttConnectMessage connect = new MqttConnectMessage(
                new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0),
                variableHeader, payload);

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
                                            case CONNACK -> {
                                                connAcks.offer((MqttConnAckMessage) msg);
                                                connAckLatch.countDown();
                                            }
                                            case SUBACK -> {
                                                subAcks.offer((MqttSubAckMessage) msg);
                                                subAckLatch.countDown();
                                            }
                                            case PUBLISH -> {
                                                MqttPublishMessage p = (MqttPublishMessage) msg;
                                                // 必须在释放前取快照
                                                inbox.offer(new Pub(
                                                        p.variableHeader().topicName(),
                                                        p.fixedHeader().isRetain(),
                                                        p.fixedHeader().isDup(),
                                                        p.fixedHeader().qosLevel().value(),
                                                        p.payload().toString(CharsetUtil.UTF_8)));
                                            }
                                            default -> {
                                            }
                                        }
                                    }
                                });
                    }
                });
        Channel channel = b.connect(HOST, PORT).sync().channel();
        channel.writeAndFlush(connect);
        return channel;
    }

    static MqttSubscribeMessage subscribe(int packetId, String filter, MqttQoS qos) {
        return new MqttSubscribeMessage(
                new MqttFixedHeader(MqttMessageType.SUBSCRIBE, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(packetId),
                new MqttSubscribePayload(List.of(new MqttTopicSubscription(filter, qos))));
    }

    static MqttSubscribeMessage subscribeMixed(int packetId, String f1, String f2) {
        List<MqttTopicSubscription> subs = new ArrayList<>();
        subs.add(new MqttTopicSubscription(f1, MqttQoS.AT_LEAST_ONCE));
        subs.add(new MqttTopicSubscription(f2, MqttQoS.AT_LEAST_ONCE));
        return new MqttSubscribeMessage(
                new MqttFixedHeader(MqttMessageType.SUBSCRIBE, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(packetId),
                new MqttSubscribePayload(subs));
    }

    static MqttPublishMessage publish(String topic, int packetId, String body, MqttQoS qos, boolean retain) {
        return new MqttPublishMessage(
                new MqttFixedHeader(MqttMessageType.PUBLISH, false, qos, retain, 0),
                new MqttPublishVariableHeader(topic, packetId),
                Unpooled.copiedBuffer(body, CharsetUtil.UTF_8));
    }

    static Pub waitPub(long seconds) throws InterruptedException {
        return inbox.poll(seconds, TimeUnit.SECONDS);
    }

    static String describe(Pub p) {
        return p == null ? "null(未收到)"
                : p.topic() + " retain=" + p.retain() + " qos=" + p.qos() + " body=" + p.body();
    }

    static void ok(String name) {
        passed++;
        System.out.println("[PASS] " + name);
    }

    static void check(String name, boolean condition, String detail) {
        if (condition) {
            ok(name);
        } else {
            fail(name + "  (" + detail + ")");
        }
    }

    static void fail(String name) {
        failed++;
        System.out.println("[FAIL] " + name);
    }
}
