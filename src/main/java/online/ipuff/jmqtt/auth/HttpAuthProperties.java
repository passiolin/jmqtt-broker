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

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import javax.validation.constraints.Min;
import java.util.Map;

/**
 * HTTP 认证配置(EMQX 契约)。
 *
 * <p>开启后 CONNECT 的认证交给外部 HTTP 服务: broker POST
 * {@code {"clientid","username","password","peerhost"}}, 服务端回
 * {@code {"result":"allow"|"deny"|"ignore","superuser":bool?}}。
 *
 * @param enabled          总开关。关闭时 AuthConfig 只装配内置静态鉴权, 零 HTTP 行为
 * @param url              认证服务地址; enabled=true 时必填, 留空启动即失败(不静默放行)
 * @param method           post(推荐, 凭据在 body)或 get(凭据走查询参数 ——
 *                         会进对端访问日志, 仅在认证服务只支持 GET 时使用)
 * @param requestTimeoutMs 单次请求超时。在连接建立路径上, 必须设小;
 *                         超时按 on-error 处理并触发熔断冷却
 * @param connectTimeoutMs TCP 连接超时
 * @param passwordHash     发给认证服务的密码形态: {@code plain}(明文, 认证服务侧才能
 *                         用任意算法校验)或 {@code sha256}(摘要, 明文不出 broker;
 *                         认证服务侧需存同样的摘要)
 * @param onError          认证服务超时/不可达/响应不合法时的策略:
 *                         {@code reject}(默认, fail-closed)或
 *                         {@code ignore}(回落到内置静态鉴权 —— 注意这意味着认证中心
 *                         故障期间仅靠 auth-username/auth-password 把门)
 * @param cooldownMs       熔断冷却时长: 一次失败后在此期间不再发请求, 直接按 on-error
 *                         处理。防止认证中心故障时每个 CONNECT 都吃满超时
 * @param headers          附加请求头(如认证服务要求的固定 Authorization)
 */
@ConfigurationProperties(prefix = "jmqtt.broker.http-auth")
public record HttpAuthProperties(
        @DefaultValue("false") boolean enabled,
        String url,
        @DefaultValue("post") String method,
        @DefaultValue("2000") @Min(100) int requestTimeoutMs,
        @DefaultValue("2000") @Min(100) int connectTimeoutMs,
        @DefaultValue("plain") String passwordHash,
        @DefaultValue("reject") String onError,
        @DefaultValue("5000") @Min(0) long cooldownMs,
        Map<String, String> headers
) {

    public boolean useGetMethod() {
        return "get".equalsIgnoreCase(method);
    }

    public boolean fallbackOnIgnore() {
        return "ignore".equalsIgnoreCase(onError);
    }
}
