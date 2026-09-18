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
package online.ipuff.jmqtt.router;

/**
 * MQTT 主题与主题过滤器的解析及校验。
 *
 * <p>主题过滤器校验容易只做「非空 + 不含非法字符」这类浅检查, 而漏掉
 * {@code #} 的个数, 漏掉了 {@code sport/tennis#} 这类「{@code #} 未独占一层」的非法过滤器,
 * 另外一旦有非法项就关闭整条连接, 而规范允许在 SUBACK 中逐项返回失败码。
 *
 * <p>这里把规则集中到一处, 供 SUBSCRIBE 逐项校验使用。
 */
public final class MqttTopic {

    /** 单层通配符 */
    public static final String SINGLE_WILDCARD = "+";

    /** 多层通配符 */
    public static final String MULTI_WILDCARD = "#";

    /** 空层, 用于 {@code a//b}、{@code /a}、{@code a/} 这类合法主题 */
    public static final String BLANK = "";

    /**
     * 主题层数上限。
     *
     * <p>必须有限制: 匹配是递归实现的, 而报文最大长度允许 10MB,
     * 一个形如 {@code a/a/a/...} 的主题可产生百万级层数, 直接打爆线程栈。
     * 后续可提升为配置项。
     */
    public static final int MAX_TOPIC_LEVELS = 128;

    private MqttTopic() {
    }

    /**
     * 切分层级。使用 {@code limit=-1} 保留尾随空层, 保证 {@code a/} 解析为 {@code ["a", ""]}。
     */
    public static String[] levels(String topicOrFilter) {
        if (topicOrFilter == null || topicOrFilter.isEmpty()) {
            return new String[]{BLANK};
        }
        return topicOrFilter.split("/", -1);
    }

    /**
     * 校验主题名(发布时使用)。主题名不得包含通配符。
     */
    public static boolean isValidTopicName(String topic) {
        if (topic == null || topic.isEmpty()) {
            return false;
        }
        if (topic.indexOf('+') >= 0 || topic.indexOf('#') >= 0) {
            return false;
        }
        return levels(topic).length <= MAX_TOPIC_LEVELS;
    }

    /**
     * 校验主题过滤器(订阅时使用)。
     *
     * <p>规则:
     * <ul>
     *   <li>{@code #} 只能出现在最后一层, 且必须独占该层({@code a/#} 合法, {@code a/b#}、{@code a/#/b} 非法)</li>
     *   <li>{@code +} 必须独占一层({@code a/+/b} 合法, {@code a/b+} 非法)</li>
     *   <li>同一层不得同时包含 {@code +} 和 {@code #}</li>
     *   <li>空层合法({@code a//b}、{@code /a})</li>
     *   <li>层数不超过 {@link #MAX_TOPIC_LEVELS}</li>
     * </ul>
     */
    public static boolean isValidFilter(String filter) {
        if (filter == null || filter.isEmpty()) {
            return false;
        }
        String[] parts = levels(filter);
        if (parts.length > MAX_TOPIC_LEVELS) {
            return false;
        }
        for (int i = 0; i < parts.length; i++) {
            String level = parts[i];
            if (level.isEmpty() || level.equals(SINGLE_WILDCARD)) {
                continue;
            }
            if (level.indexOf('#') >= 0) {
                if (!level.equals(MULTI_WILDCARD)) {
                    // 形如 a/b# —— # 未独占一层
                    return false;
                }
                if (i != parts.length - 1) {
                    // # 不是最后一层
                    return false;
                }
                continue;
            }
            if (level.indexOf('+') >= 0) {
                // 形如 a/b+ —— + 未独占一层
                return false;
            }
        }
        return true;
    }

    /**
     * 判断该层是否以 {@code $} 开头(系统主题)。
     *
     * <p>MQTT 规范要求: 以通配符开头的过滤器不得匹配 {@code $} 开头的主题。
     */
    public static boolean isMetadataLevel(String level) {
        return level != null && level.startsWith("$");
    }
}
