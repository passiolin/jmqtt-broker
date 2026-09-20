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
 */
package online.ipuff.jmqtt;

import online.ipuff.jmqtt.admin.AdminProperties;
import online.ipuff.jmqtt.auth.HttpAuthProperties;
import online.ipuff.jmqtt.authz.HttpAclProperties;
import online.ipuff.jmqtt.config.BrokerProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * jmqtt-broker 启动入口。
 *
 * <p>由 Spring Boot 驱动启动流程,
 * 这里替换为 Spring Boot 的标准启动方式。
 *
 * <p>两个配置类都是<b>无条件注册</b>的: 它们对应的功能(管理面 / Redis 持久化)
 * 由各自的 {@code enabled} 开关与依赖是否可用决定, 而不是由 Bean 存不存在决定。
 * 用 Bean 的有无来表达「功能开不开」曾经造成过一个隐蔽故障 ——
 * 默认配置下整个应用起不来, 而所有测试都是带着显式开关跑的, 谁都没发现。
 */
@EnableAsync
@EnableScheduling
@SpringBootApplication
@EnableConfigurationProperties({BrokerProperties.class, AdminProperties.class,
        HttpAuthProperties.class, HttpAclProperties.class})
public class JmqttBrokerApplication {

    public static void main(String[] args) {
        SpringApplication.run(JmqttBrokerApplication.class, args);
    }
}
