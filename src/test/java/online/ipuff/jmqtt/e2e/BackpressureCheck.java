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
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
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
 * MQTT 流控窗口验证。用法: BackpressureCheck <端口> <maxInflight>
 *
 * <p>验证的是规范行为: 服务端不得让未确认的 QoS 1 消息超过在途窗口上限。
 * 用「只收不回」的客户端即可观测 —— 收到几条就应该恰好等于窗口大小。
 */
public class BackpressureCheck {

    static final String HOST = "127.0.0.1";
    static final String TOPIC = "bp/room1/data";
    static final int PUBLISH_COUNT = 20;

    record Delivery(int packetId, String body) {
    }

    static final BlockingQueue<Delivery> deliveries = new LinkedBlockingQueue<>();
    static final BlockingQueue<MqttConnAckMessage> connAcks = new LinkedBlockingQueue<>();

    static int passed = 0;
    static int failed = 0;

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);
        int maxInflight = Integer.parseInt(args[1]);
        EventLoopGroup group = new NioEventLoopGroup(2);
        try {
            System.out.println("=== MQTT 流控窗口验证 (maxInflight=" + maxInflight + ") ===");

            Channel subscriber = connect(group, port, "bp-sub", true);
            awaitConnAck();
            subscriber.writeAndFlush(subscribe(1, "bp/+/data", MqttQoS.AT_LEAST_ONCE));
            Thread.sleep(800);

            Channel publisher = connect(group, port, "bp-pub", true);
            awaitConnAck();
            Thread.sleep(400);

            for (int i = 1; i <= PUBLISH_COUNT; i++) {
                publisher.writeAndFlush(publish(TOPIC, 200 + i, "m" + i));
            }

            // ---- 关键断言 1: 一条都不确认时, 收到的条数应恰好等于窗口上限 ----
            Thread.sleep(2500);
            List<Delivery> firstBatch = drain();
            check("不确认时收到的条数恰好等于窗口上限(" + maxInflight + " 条)",
                    firstBatch.size() == maxInflight,
                    "实际收到 " + firstBatch.size() + " 条 —— 说明流控未生效");

            // ---- 关键断言 2: 确认一批后, 应继续收到下一批 ----
            ack(subscriber, firstBatch);
            Thread.sleep(1500);
            List<Delivery> secondBatch = drain();
            check("确认一批后应继续投递下一批",
                    secondBatch.size() == maxInflight,
                    "实际收到 " + secondBatch.size() + " 条");

            // ---- 关键断言 3: 持续确认可以把全部消息收完(队列不丢) ----
            int total = firstBatch.size() + secondBatch.size();
            for (int round = 0; round < 10 && total < PUBLISH_COUNT; round++) {
                ack(subscriber, secondBatch);
                Thread.sleep(900);
                secondBatch = drain();
                total += secondBatch.size();
            }
            check("持续确认后应收完全部 " + PUBLISH_COUNT + " 条(队列未丢消息)",
                    total == PUBLISH_COUNT, "实际共收到 " + total + " 条");

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
                                            case CONNACK -> connAcks.offer((MqttConnAckMessage) msg);
                                            case PUBLISH -> {
                                                MqttPublishMessage p = (MqttPublishMessage) msg;
                                                // 刻意不回 PUBACK: 用来观测服务端的在途窗口
                                                deliveries.offer(new Delivery(
                                                        p.variableHeader().packetId(),
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

    static void awaitConnAck() throws InterruptedException {
        connAcks.poll(10, TimeUnit.SECONDS);
    }

    static void ack(Channel channel, List<Delivery> batch) {
        for (Delivery d : batch) {
            channel.writeAndFlush(new MqttPubAckMessage(
                    new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                    MqttMessageIdVariableHeader.from(d.packetId())));
        }
    }

    static List<Delivery> drain() throws InterruptedException {
        List<Delivery> list = new ArrayList<>();
        Delivery d;
        while ((d = deliveries.poll(250, TimeUnit.MILLISECONDS)) != null) {
            list.add(d);
        }
        return list;
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

    static void check(String name, boolean condition, String detail) {
        if (condition) {
            passed++;
            System.out.println("[PASS] " + name);
        } else {
            failed++;
            System.out.println("[FAIL] " + name + "  (" + detail + ")");
        }
    }
}
