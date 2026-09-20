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

/**
 * 认证结果。
 *
 * @param allowed   是否放行连接
 * @param superuser 超级用户(EMQX 语义): 认证服务显式声明时置位,
 *                  该连接后续跳过全部 ACL 检查。内置静态鉴权恒为 false
 */
public record AuthResult(boolean allowed, boolean superuser) {

    public static final AuthResult ALLOW = new AuthResult(true, false);

    public static final AuthResult DENY = new AuthResult(false, false);
}
