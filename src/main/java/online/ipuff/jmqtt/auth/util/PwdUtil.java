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
package online.ipuff.jmqtt.auth.util;

import online.ipuff.jmqtt.auth.AuthService;

/**
 * 生成 {@code jmqtt.broker.auth-password} 的配置值。
 *
 * <p>认证采用摘要比对后不再需要任何私钥, 这个工具只负责把明文密码转成配置里要填的摘要值。
 *
 * <pre>
 *   java -cp jmqtt-broker.jar online.ipuff.jmqtt.auth.util.PwdUtil &lt;明文密码&gt;
 *   =&gt; sha256:2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b
 * </pre>
 */
public final class PwdUtil {

    private PwdUtil() {
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("用法: PwdUtil <明文密码>");
            System.exit(1);
        }
        String plain = args[0];
        System.out.println("auth-password: sha256:" + AuthService.sha256Hex(plain));
    }
}
