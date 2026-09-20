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

import java.util.concurrent.CompletableFuture;

/**
 * ACL(授权)服务: 发布/订阅前的权限判定。
 *
 * <p>实现方若命中缓存或整体关闭, 必须返回<b>已完成</b>的 future ——
 * 调用方({@code PublishHandler}/{@code SubscribeHandler})据此走零开销同步路径,
 * 不进入挂起门控。
 */
public interface IAclService {

    /**
     * 判定一次 publish / subscribe。
     *
     * @param clientId 客户端标识
     * @param username 认证时的用户名(可能为 null)
     * @param peerhost 对端 IP(可能为 null)
     * @param action   {@code publish} 或 {@code subscribe}
     * @param topic    发布的主题名 / 订阅的过滤器
     * @return 是否放行; future 不应异常完成, 出错语义由实现收敛为 false 或策略值
     */
    CompletableFuture<Boolean> check(String clientId, String username, String peerhost,
                                     String action, String topic);

    /**
     * 客户端断连: 释放其相关状态(如决策缓存)。由连接关闭路径调用。
     */
    void onClientOffline(String clientId);
}
