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
package online.ipuff.jmqtt.session;

import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * QoS 分布统计。
 *
 * <h2>为什么需要它</h2>
 * 「在途消息要不要持久化」的答案完全取决于 <b>QoS 1/2 的实际占比</b>:
 * <ul>
 *   <li>若设备侧以 QoS 0 为主、只有下行指令用 QoS 1, 那么每条 QoS 1/2 多一次 Redis 往返
 *       的开销可以忽略 —— 选同步写(不丢)</li>
 *   <li>若上行也用 QoS 1, 投递速率上去了, 同步写会明显抬高延迟 ——
 *       必须用写回并接受一个丢弃窗口</li>
 * </ul>
 * 这个占比<b>只能从真实流量里长出来</b>, 事后补埋点就丢掉了最有价值的那段数据。
 *
 * <p>{@code deliveredQos12Permille} 是百分比的十倍(千分数), 用整数表示避免浮点:
 * 137 表示 13.7% 的投递是 QoS 1/2。
 */
@Component
public class QosMetrics {

    private final AtomicLong publishedQos0 = new AtomicLong();
    private final AtomicLong publishedQos1 = new AtomicLong();
    private final AtomicLong publishedQos2 = new AtomicLong();
    private final AtomicLong deliveredQos0 = new AtomicLong();
    private final AtomicLong deliveredQos1 = new AtomicLong();
    private final AtomicLong deliveredQos2 = new AtomicLong();

    /** 入站: 客户端发布的报文 */
    public void published(int qos) {
        switch (qos) {
            case 1 -> publishedQos1.incrementAndGet();
            case 2 -> publishedQos2.incrementAndGet();
            default -> publishedQos0.incrementAndGet();
        }
    }

    /** 出站: 服务端投递给客户端的报文 */
    public void delivered(int qos) {
        switch (qos) {
            case 1 -> deliveredQos1.incrementAndGet();
            case 2 -> deliveredQos2.incrementAndGet();
            default -> deliveredQos0.incrementAndGet();
        }
    }

    public Map<String, Long> stats() {
        long p0 = publishedQos0.get();
        long p1 = publishedQos1.get();
        long p2 = publishedQos2.get();
        long d0 = deliveredQos0.get();
        long d1 = deliveredQos1.get();
        long d2 = deliveredQos2.get();

        long publishedTotal = p0 + p1 + p2;
        long deliveredTotal = d0 + d1 + d2;

        Map<String, Long> stats = new LinkedHashMap<>();
        stats.put("publishedQos0", p0);
        stats.put("publishedQos1", p1);
        stats.put("publishedQos2", p2);
        stats.put("deliveredQos0", d0);
        stats.put("deliveredQos1", d1);
        stats.put("deliveredQos2", d2);
        // 千分数, 137 = 13.7%。这个数字直接决定在途消息持久化该用哪种模式
        stats.put("deliveredQos12Permille",
                deliveredTotal == 0 ? 0 : (d1 + d2) * 1000 / deliveredTotal);
        stats.put("publishedQos12Permille",
                publishedTotal == 0 ? 0 : (p1 + p2) * 1000 / publishedTotal);
        return stats;
    }
}
