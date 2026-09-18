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
package online.ipuff.jmqtt.auth;

/**
 * 连接认证服务。
 *
 * 对接设备库 / LDAP / HTTP 认证中心。
 */
public interface IAuthService {

    /**
     * 校验用户名与密码。
     *
     * @param username 客户端提交的用户名
     * @param password 客户端提交的密码
     * @return 是否通过
     */
    boolean checkValid(String username, String password);
}
