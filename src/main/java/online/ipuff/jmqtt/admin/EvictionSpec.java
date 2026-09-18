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
package online.ipuff.jmqtt.admin;

/**
 * 驱逐请求(控制台下发, 从命令 JSON 解析)。
 *
 * <p>这是一份<b>未经验证的意图</b>, 不是可执行的计划: 具体要断开多少个、每批几个、
 * 间隔多久, 都要结合节点当前的连接数与配置上限才能定下来。所以这里只承载请求字段,
 * 规范化与裁剪由 {@link EvictionService} 在做, 结果放进 {@link EvictionTask}。
 *
 * @param mode           选取方式。{@code ratio} = 按当前连接数的比例, {@code count} = 按绝对数量
 * @param value          与 {@code mode} 配套的值。ratio 时为 0~1 的小数, count 时为条数
 * @param batchSize      每批断开的数量; 为空取配置默认值
 * @param intervalMs     批次间隔; 为空取配置默认值, 且不得低于配置下限
 * @param publishWill    是否按规范发布遗嘱。为空视为 {@code true}
 * @param clientIdPrefix 只驱逐匹配该前缀的客户端; 为空表示不筛选。
 *                       用途是灰度: 先拿一小批设备验证整条链路, 再全量做
 */
public record EvictionSpec(
        String mode,
        Double value,
        Integer batchSize,
        Integer intervalMs,
        Boolean publishWill,
        String clientIdPrefix
) {

    public static final String MODE_RATIO = "ratio";
    public static final String MODE_COUNT = "count";

    public boolean byRatio() {
        return MODE_RATIO.equalsIgnoreCase(mode);
    }

    public boolean byCount() {
        return MODE_COUNT.equalsIgnoreCase(mode);
    }

    /**
     * 校验请求本身是否自洽。<b>只校验语义, 不校验数量上限</b> ——
     * 上限依赖节点当前的连接数, 那要等选完候选集才知道。
     */
    public void validate() {
        if (!byRatio() && !byCount()) {
            throw new IllegalArgumentException("mode 必须是 " + MODE_RATIO + " 或 " + MODE_COUNT);
        }
        if (value == null) {
            throw new IllegalArgumentException("缺少 value");
        }
        if (byRatio() && (value <= 0 || value > 1)) {
            throw new IllegalArgumentException("ratio 必须落在 (0, 1] 区间, 当前: " + value);
        }
        if (byCount() && value < 1) {
            throw new IllegalArgumentException("count 必须 >= 1, 当前: " + value);
        }
    }

    /** 是否按规范发布遗嘱。未指定即规范行为。 */
    public boolean publishWillOrTrue() {
        return publishWill == null || publishWill;
    }
}
