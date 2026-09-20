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
package online.ipuff.jmqtt.auth;

import online.ipuff.jmqtt.config.BrokerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 认证装配。
 *
 * <p>{@link AuthService} 不再标 {@code @Service}: {@link IAuthService} 的 Bean
 * 只在这里定义, 避免多个实现竞争注入 —— 「内置 / HTTP / HTTP+内置链」的切换
 * 是一个纯配置决策, 不该散落在组件扫描里。
 *
 * <p>HTTP 认证开启时装配 {@link HttpAuthService}(认证链: HTTP 结果 ignore 或
 * 出错按策略回落到内置静态鉴权); 关闭时退回内置实现。
 */
@Configuration
public class AuthConfig {

    private static final Logger log = LoggerFactory.getLogger(AuthConfig.class);

    @Bean
    public IAuthService authService(BrokerProperties properties, HttpAuthProperties httpAuth) {
        AuthService internal = new AuthService(properties);
        if (httpAuth.enabled()) {
            // url 为空时 HttpAuthService 构造即失败 —— fail-fast, 不静默放行
            log.info("认证实现: HTTP 认证中心({}) + 内置静态鉴权(ignore/故障回落), on-error={}",
                    httpAuth.url(), httpAuth.onError());
            return new HttpAuthService(httpAuth, internal);
        }
        log.info("认证实现: 内置静态鉴权(auth-username/auth-password)");
        return internal;
    }
}
