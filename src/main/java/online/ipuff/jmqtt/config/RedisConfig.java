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
package online.ipuff.jmqtt.config;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Redis 客户端配置。
 *
 * <p>刻意<b>不</b>在启动时建立连接: Redis 挂掉不应该导致 broker 起不来。
 * {@link RedisClient} 本身只是客户端工厂, 真正的连接由
 * {@code RedisSessionRepository} 在首次使用时惰性建立, 失败后靠健康检查反复重试。
 *
 * <p>连接是单条、线程安全、多路复用的 —— 会话相关的命令频率很低,
 * 不需要连接池; 池化反而会引入额外的连接管理成本。
 */
@Configuration
@ConditionalOnProperty(prefix = "jmqtt.broker.redis", name = "enabled", havingValue = "true")
public class RedisConfig {

    private static final Logger log = LoggerFactory.getLogger(RedisConfig.class);

    @Bean(destroyMethod = "shutdown")
    public RedisClient sessionRedisClient(BrokerProperties properties) {
        BrokerProperties.RedisProperties redis = properties.redis();

        RedisURI.Builder builder = RedisURI.Builder
                .redis(redis.host(), redis.port())
                .withDatabase(redis.database());
        if (redis.password() != null && !redis.password().isBlank()) {
            builder.withPassword(redis.password());
        }

        RedisClient client = RedisClient.create(builder.build());
        // 命令超时必须设小: 这些命令都在连接建立路径上, Redis 卡住会直接拖慢握手
        client.setDefaultTimeout(Duration.ofMillis(redis.commandTimeoutMs()));
        log.info("Redis 客户端已创建(惰性连接): {}:{}", redis.host(), redis.port());
        return client;
    }
}
