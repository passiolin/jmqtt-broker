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
package online.ipuff.jmqtt.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import online.ipuff.jmqtt.config.BrokerProperties;
import online.ipuff.jmqtt.handler.MqttBrokerHandler;
import online.ipuff.jmqtt.server.codec.MqttWebSocketCodec;
import online.ipuff.jmqtt.session.ConnectionRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Netty 接入层, 负责启动 MQTT/TCP 与 MQTT/WebSocket 两个监听器。
 *
 * <p>实现 Spring 的 {@link SmartLifecycle}, 由容器驱动启动与优雅关闭 ——
 * 这样 MQTT 接入层的生命周期与 Web 容器、各存储客户端处在同一个编排里,
 * 不需要额外的启动钩子。
 *
 * <p><b>注意生命周期顺序</b>: EventLoopGroup 必须在 {@code stop()} 里
 * {@code shutdownGracefully()} 并等待, 否则 Spring 关闭时线程不会退出, 容器会挂住。
 */
@Component
public class BrokerServer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(BrokerServer.class);

    private final BrokerProperties properties;
    private final MqttBrokerHandler brokerHandler;
    private final ConnectionRegistry connectionRegistry;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel mqttChannel;
    private Channel webSocketChannel;
    private boolean useEpoll;

    public BrokerServer(BrokerProperties properties,
                        MqttBrokerHandler brokerHandler,
                        ConnectionRegistry connectionRegistry) {
        this.properties = properties;
        this.brokerHandler = brokerHandler;
        this.connectionRegistry = connectionRegistry;
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            log.info("正在启动 MQTT Broker [{}] ...", properties.id());

            this.useEpoll = properties.useEpoll() && Epoll.isAvailable();
            if (properties.useEpoll() && !useEpoll) {
                log.warn("配置了 use-epoll 但当前环境不支持, 已自动降级为 NIO");
            }

            this.bossGroup = useEpoll
                    ? new EpollEventLoopGroup(properties.bossThreadsOrDefault())
                    : new NioEventLoopGroup(properties.bossThreadsOrDefault());
            this.workerGroup = useEpoll
                    ? new EpollEventLoopGroup(properties.workerThreadsOrDefault())
                    : new NioEventLoopGroup(properties.workerThreadsOrDefault());

            startMqttServer();
            if (properties.websocketEnabled()) {
                startWebSocketServer();
                log.info("MQTT Broker [{}] 已启动. 端口: {} WebSocket 端口: {} (transport={}, boss={}, worker={})",
                        properties.id(), properties.port(), properties.websocketPort(),
                        useEpoll ? "epoll" : "nio",
                        properties.bossThreadsOrDefault(), properties.workerThreadsOrDefault());
            } else {
                log.info("MQTT Broker [{}] 已启动. 端口: {} (transport={}, boss={}, worker={})",
                        properties.id(), properties.port(), useEpoll ? "epoll" : "nio",
                        properties.bossThreadsOrDefault(), properties.workerThreadsOrDefault());
            }
        } catch (Exception e) {
            running.set(false);
            shutdownGroups();
            throw new IllegalStateException("MQTT Broker 启动失败", e);
        }
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        log.info("正在停止 MQTT Broker [{}] ...", properties.id());

        // 先关闭所有客户端连接, 让客户端感知断开并触发重连
        connectionRegistry.closeAll();

        if (mqttChannel != null) {
            mqttChannel.close().awaitUninterruptibly();
            mqttChannel = null;
        }
        if (webSocketChannel != null) {
            webSocketChannel.close().awaitUninterruptibly();
            webSocketChannel = null;
        }
        shutdownGroups();
        log.info("MQTT Broker [{}] 已停止.", properties.id());
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    private void shutdownGroups() {
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
            bossGroup = null;
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
            workerGroup = null;
        }
    }

    private ServerBootstrap baseBootstrap(ChannelInitializer<SocketChannel> childInitializer) {
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(useEpoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .childHandler(childInitializer)
                .option(ChannelOption.SO_BACKLOG, properties.soBacklog())
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.SO_KEEPALIVE, properties.soKeepAlive())
                .childOption(ChannelOption.TCP_NODELAY, properties.tcpNoDelay())
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(
                        properties.writeBufferLowWaterMark(),
                        properties.writeBufferHighWaterMark()));
        return bootstrap;
    }

    private Channel bind(ServerBootstrap bootstrap, int port) throws InterruptedException {
        if (StringUtils.hasText(properties.host())) {
            return bootstrap.bind(properties.host(), port).sync().channel();
        }
        return bootstrap.bind(port).sync().channel();
    }

    private void startMqttServer() throws InterruptedException {
        ServerBootstrap bootstrap = baseBootstrap(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel socketChannel) {
                socketChannel.pipeline()
                        // 默认心跳; CONNECT 之后会按客户端协商的 keepAlive 重建
                        .addFirst("idle", new IdleStateHandler(0, 0, properties.defaultKeepAlive()))
                        .addLast("mqttDecoder", new MqttDecoder(properties.maxPayloadSize()))
                        .addLast("mqttEncoder", MqttEncoder.INSTANCE)
                        .addLast("broker", brokerHandler);
            }
        });
        this.mqttChannel = bind(bootstrap, properties.port());
    }

    private void startWebSocketServer() throws InterruptedException {
        ServerBootstrap bootstrap = baseBootstrap(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel socketChannel) {
                socketChannel.pipeline()
                        .addFirst("idle", new IdleStateHandler(0, 0, properties.defaultKeepAlive()))
                        .addLast("httpCodec", new HttpServerCodec())
                        .addLast("httpAggregator", new HttpObjectAggregator(1 << 20))
                        .addLast("httpCompressor", new HttpContentCompressor())
                        .addLast("webSocketProtocol", new WebSocketServerProtocolHandler(
                                properties.websocketPath(), "mqtt,mqttv3.1,mqttv3.1.1", true, 65536))
                        .addLast("mqttWebSocket", new MqttWebSocketCodec())
                        .addLast("mqttDecoder", new MqttDecoder(properties.maxPayloadSize()))
                        .addLast("mqttEncoder", MqttEncoder.INSTANCE)
                        .addLast("broker", brokerHandler);
            }
        });
        this.webSocketChannel = bind(bootstrap, properties.websocketPort());
    }
}
