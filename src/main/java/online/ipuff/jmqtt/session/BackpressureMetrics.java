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
 * 背压相关的累计计数器。
 *
 * <p>这些数字是「broker 正在被迫丢消息」的唯一可见信号。三条各自的含义不同,
 * 排查方向也不同:
 *
 * <table>
 *   <tr><th>指标</th><th>含义</th><th>通常说明</th></tr>
 *   <tr><td>{@code qos0NotWritableDropped}</td><td>QoS 0 因写缓冲满被丢</td>
 *       <td>客户端读取慢。QoS 0 无保证, 丢弃符合预期</td></tr>
 *   <tr><td>{@code sendQueueFullDropped}</td><td>发送队列超限丢最旧</td>
 *       <td><b>投递速度持续超过客户端消费速度</b>, 需要排查该客户端</td></tr>
 *   <tr><td>{@code offlineQueueDropped}</td><td>离线队列超限丢最旧</td>
 *       <td>客户端离线太久或订阅了高频主题</td></tr>
 * </table>
 *
 * <p>{@code sendQueueFullDropped} 持续增长是最需要关注的 —— 它意味着有客户端的
 * 确认速度跟不上, 而窗口机制已经在生效(否则不会积压到队列上限)。
 */
@Component
public class BackpressureMetrics {

    private final AtomicLong qos0NotWritableDropped = new AtomicLong();
    private final AtomicLong sendQueueFullDropped = new AtomicLong();
    private final AtomicLong sendQueueEnqueued = new AtomicLong();

    /** QoS 0 因 channel 不可写被丢弃 */
    public void qos0NotWritableDropped() {
        qos0NotWritableDropped.incrementAndGet();
    }

    /** 发送队列超限丢弃 */
    public void queueFullDropped() {
        queueFullDropped(1);
    }

    public void queueFullDropped(int count) {
        sendQueueFullDropped.addAndGet(count);
    }

    /** 因窗口用尽而进入排队的消息数 —— 反映流控在多大程度上生效 */
    public void sendQueueEnqueued() {
        sendQueueEnqueued.incrementAndGet();
    }

    public Map<String, Long> stats() {
        Map<String, Long> stats = new LinkedHashMap<>();
        stats.put("qos0NotWritableDropped", qos0NotWritableDropped.get());
        stats.put("sendQueueEnqueued", sendQueueEnqueued.get());
        stats.put("sendQueueFullDropped", sendQueueFullDropped.get());
        return stats;
    }
}
