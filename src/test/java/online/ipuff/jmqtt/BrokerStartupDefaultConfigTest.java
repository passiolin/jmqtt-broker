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
package online.ipuff.jmqtt;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * 默认配置下的启动冒烟测试。
 *
 * <h2>它为什么必须存在</h2>
 * {@code application.yml} 里 {@code jmqtt.broker.redis.enabled} 默认是 {@code false}。
 * 曾经有一轮把 {@code InflightPersistence} 做成
 * {@code @ConditionalOnProperty(redis.enabled=true)}, 而 {@code ConnectHandler}
 * 又直接注入它 —— 于是<b>默认配置下应用根本起不来</b>, 报
 * {@code NoSuchBeanDefinitionException}。当时所有端到端脚本都显式带了
 * {@code --jmqtt.broker.redis.enabled=true}, 所以一路都是绿的, 直到跑一个
 * 不启用 Redis 的用例才暴露。
 *
 * <p>这个测试就是那条缺失的用例: 用 <b>application.yml 原样</b>的配置把上下文拉起来。
 * 它不校验任何业务行为, 只校验一件事 —— 「默认配置能启动」。
 * 这一类回归(条件 Bean 被硬注入、必需属性缺失、循环依赖)只有真正 refresh 一次上下文才看得见。
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                // 端口不能用 0: 配置上有 @Min(1) 约束。挑高位端口避免撞上并行跑的用例
                "jmqtt.broker.port=21883",
                "jmqtt.broker.websocket-enabled=false"
        })
class BrokerStartupDefaultConfigTest {

    @Test
    @DisplayName("默认配置(redis.enabled=false)下上下文可以启动")
    void contextLoadsWithDefaultConfig() {
        // 能跑到这里就说明上下文 refresh 成功
    }
}
