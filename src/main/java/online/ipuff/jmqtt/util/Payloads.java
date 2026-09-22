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
package online.ipuff.jmqtt.util;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

/**
 * 日志用的 payload 预览 —— <b>只为日志服务, 不参与任何业务解析</b>。
 *
 * <p>两条硬约束:
 * <ol>
 *   <li><b>截断</b>: 打印上限之外的长度只标注总大小。IoT payload 上限是
 *       {@code max-payload-size}(默认 10MB), 全量打印等于用日志把磁盘写满。</li>
 *   <li><b>有损但不抛错</b>: 二进制 payload 按 UTF-8 解码, 不可见字符替换为 {@code ?} ——
 *       日志路径绝不能因为 payload 内容抛异常。</li>
 * </ol>
 */
public final class Payloads {

    /** 单条日志打印的 payload 上限(字符) */
    public static final int LOG_PREVIEW_LIMIT = 256;

    private Payloads() {
    }

    public static String preview(byte[] payload) {
        if (payload == null || payload.length == 0) {
            return "";
        }
        return render(payload, Math.min(payload.length, LOG_PREVIEW_LIMIT), payload.length);
    }

    /** 不复制整个 buffer, 只取预览段 —— 10MB 的 PUBLISH 打日志也不该多一次全量拷贝 */
    public static String preview(ByteBuf payload) {
        if (payload == null || payload.readableBytes() == 0) {
            return "";
        }
        int total = payload.readableBytes();
        int len = Math.min(total, LOG_PREVIEW_LIMIT);
        byte[] chunk = new byte[len];
        // getBytes 不移动 readerIndex, 不影响后续的业务读取
        payload.getBytes(payload.readerIndex(), chunk);
        return render(chunk, len, total);
    }

    private static String render(byte[] chunk, int len, int total) {
        StringBuilder sb = new StringBuilder(len + 32);
        for (int i = 0; i < len; i++) {
            char c = (char) (chunk[i] & 0xFF);
            // 可见 ASCII 与常见空白保留, 其余(多字节 UTF-8 的组成部分、控制字符)替换
            sb.append(c >= 0x20 && c < 0x7F ? c : (c == '\n' || c == '\t' ? ' ' : '?'));
        }
        if (total > len) {
            sb.append("...(truncated, total=").append(total).append("B)");
        }
        return sb.toString();
    }
}
