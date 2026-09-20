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

import java.util.concurrent.CompletableFuture;

/**
 * 连接认证服务。
 *
 * <p>对接设备库 / LDAP / HTTP 认证中心(见 {@link HttpAuthService}, EMQX 契约)。
 *
 * <p>异步签名是刻意的: HTTP 认证中心的响应不能阻塞 Netty EventLoop,
 * CONNECT 处理在等结果期间挂起。实现方若无需外部调用(如内置静态鉴权),
 * 必须返回<b>已完成</b>的 future —— 调用方据此走零开销同步路径。
 */
public interface IAuthService {

    /**
     * 认证一个 CONNECT。
     *
     * @param clientId 客户端标识(v5 下可能是服务端刚分配的)
     * @param username 客户端提交的用户名, 可能为 null
     * @param password 客户端提交的密码(UTF-8 解码), 可能为 null
     * @param peerhost 对端 IP, 可能为 null(如 EmbeddedChannel); 认证服务常按来源网段放行
     * @return 认证结果; 异常视为拒绝, 不要让 future 异常完成
     */
    CompletableFuture<AuthResult> authenticate(String clientId, String username, String password, String peerhost);
}
