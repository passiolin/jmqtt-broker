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
package online.ipuff.jmqtt.authz;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import javax.validation.constraints.Min;
import java.util.Map;

/**
 * HTTP ACL(授权)配置(EMQX 契约)。
 *
 * <p>开启后每次订阅(逐过滤器)与每条 PUBLISH 前询问外部服务:
 * POST {@code {"clientid","username","peerhost","action","topic"}},
 * 服务端回 {@code {"result":"allow"|"deny"|"ignore"}}。
 * superuser 连接(认证时授予)整体跳过, 不发请求。
 *
 * @param enabled                   总开关。关闭时 {@link HttpAclService} 恒为 allow,
 *                                  发布/订阅路径零额外行为
 * @param url                       ACL 服务地址; enabled=true 时必填, 留空启动即失败
 * @param method                    post 或 get
 * @param requestTimeoutMs          单次请求超时。必须设小: 这条路径上有 PUBLISH
 * @param connectTimeoutMs          TCP 连接超时
 * @param cacheTtlSeconds           决策缓存时长(秒)。0 = 不缓存。
 *                                  <b>缓存是本功能的可行性前提</b> —— 没有它,
 *                                  每条 PUBLISH 都要吃一次 HTTP RTT
 * @param cacheMaxEntriesPerClient  单客户端缓存条目上限(LRU 挤出最旧)。
 *                                  防止一个订阅了几千个主题的客户端把缓存撑爆
 * @param onFail                    {@code ignore} 结果与故障(超时/不可达/响应不合法)
 *                                  时的策略: {@code deny}(默认 fail-closed)或 {@code allow}
 * @param cooldownMs                熔断冷却: 故障期间不发请求直接按 on-fail 结束
 * @param headers                   附加请求头
 */
@ConfigurationProperties(prefix = "jmqtt.broker.http-acl")
public record HttpAclProperties(
        @DefaultValue("false") boolean enabled,
        String url,
        @DefaultValue("post") String method,
        @DefaultValue("2000") @Min(100) int requestTimeoutMs,
        @DefaultValue("2000") @Min(100) int connectTimeoutMs,
        @DefaultValue("60") @Min(0) long cacheTtlSeconds,
        @DefaultValue("32") @Min(1) int cacheMaxEntriesPerClient,
        @DefaultValue("deny") String onFail,
        @DefaultValue("5000") @Min(0) long cooldownMs,
        Map<String, String> headers
) {

    public boolean useGetMethod() {
        return "get".equalsIgnoreCase(method);
    }

    public boolean failOpen() {
        return "allow".equalsIgnoreCase(onFail);
    }
}
