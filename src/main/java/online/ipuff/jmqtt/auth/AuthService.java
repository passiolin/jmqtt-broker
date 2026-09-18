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

import online.ipuff.jmqtt.config.BrokerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * 基于配置的认证实现。
 *
 * <p><b>这里刻意采用标准的凭据校验, 而不是「把用户名做一次可逆变换当密码用」。</b>
 * 那种做法(用固定私钥加密用户名再比对密文)本质上是把用户名本身当成了不可吊销、
 * 不可轮换的固定凭据 —— 想换密码必须换用户名; 而且 PKCS#1 v1.5 填充带随机性,
 * 同一个用户名每次生成的密文并不相同, 比对逻辑很容易写成永远不成立。
 *
 * <p>配置形式:
 * <pre>
 *   jmqtt.broker.auth-password: secret                    # 明文, 仅用于本地联调
 *   jmqtt.broker.auth-password: sha256:2bb80d537b1d...    # SHA-256 十六进制摘要, 推荐
 * </pre>
 *
 * <p>生成摘要: {@code java -cp jmqtt-broker.jar online.ipuff.jmqtt.auth.util.PwdUtil <明文>}
 *
 * <p>生产环境请替换为对接设备库 / LDAP / HTTP 认证中心的实现 —— 接入点只有
 * {@link IAuthService} 这一个接口。
 */
@Service
public class AuthService implements IAuthService {

    private static final Logger log = LoggerFactory.getLogger(AuthService.class);

    private static final String SHA256_PREFIX = "sha256:";

    private final BrokerProperties properties;

    public AuthService(BrokerProperties properties) {
        this.properties = properties;
    }

    @Override
    public boolean checkValid(String username, String password) {
        if (!properties.authEnabled()) {
            return true;
        }
        if (username == null || username.isEmpty() || password == null) {
            return false;
        }

        String expectedUsername = properties.authUsername();
        if (expectedUsername == null
                || !constantTimeEquals(bytes(username), bytes(expectedUsername))) {
            log.debug("认证失败: 用户名不匹配, username={}", username);
            return false;
        }

        String configured = properties.authPassword();
        if (configured == null) {
            log.warn("认证失败: 未配置 auth-password");
            return false;
        }

        boolean ok;
        if (configured.startsWith(SHA256_PREFIX)) {
            byte[] expected = HexFormat.of().parseHex(configured.substring(SHA256_PREFIX.length()));
            ok = constantTimeEquals(sha256(password), expected);
        } else {
            ok = constantTimeEquals(bytes(password), bytes(configured));
        }

        if (!ok) {
            log.debug("认证失败: 密码不匹配, username={}", username);
        }
        return ok;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 计算 SHA-256 摘要。
     */
    public static byte[] sha256(String plain) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes(plain));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 不可用", e);
        }
    }

    /**
     * 计算 SHA-256 十六进制摘要, 供 {@code sha256:} 形式的配置使用。
     */
    public static String sha256Hex(String plain) {
        return HexFormat.of().formatHex(sha256(plain));
    }

    /**
     * 常量时间字节比较, 避免通过比较耗时泄露信息。
     */
    static boolean constantTimeEquals(byte[] a, byte[] b) {
        if (a.length != b.length) {
            return false;
        }
        int diff = 0;
        for (int i = 0; i < a.length; i++) {
            diff |= a[i] ^ b[i];
        }
        return diff == 0;
    }
}
