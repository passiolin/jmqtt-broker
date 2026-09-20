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
package online.ipuff.jmqtt.cluster.kafka;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 分区消费位点的完成跟踪 —— 多线程并行消费的安全边界。
 *
 * <p>Kafka 只保证分区内有序, 消费侧按分区分派 worker 后, 同一分区的记录
 * 仍然按序处理(FIFO 队列), 但「整批处理完再 commitSync」不再成立 ——
 * 位点提交必须只领先于<b>已连续完成</b>的部分:
 *
 * <ul>
 *   <li>{@link #submitted}: poller 把记录交给 worker 前登记, 建立位点的起点</li>
 *   <li>{@link #completed}: worker 处理完一条记录后上报(不同分区并发,
 *       同一分区串行有序)</li>
 *   <li>{@link #drainCommittable}: 取出「自上次提交以来连续完成」推进到的位点;
 *       高位 offset 先完成时<b>不得</b>推进 —— 否则中间未完成的消息在宕机重放时
 *       就静默丢失了, 至少一次语义被破坏</li>
 * </ul>
 *
 * <p>线程模型: {@code submitted}/{@code drainCommittable} 只在 poller 线程调用,
 * {@code completed} 在各 worker 线程并发调用 —— 每个分区独立状态, 状态内部同步,
 * 临界区只有几次堆操作。
 */
final class PartitionProgressTracker {

    private final ConcurrentHashMap<TopicPartition, PartitionState> states = new ConcurrentHashMap<>();

    /** poller 线程: 记录已被分派。首个 offset 建立该分区的位点起点 */
    void submitted(TopicPartition tp, long offset) {
        states.computeIfAbsent(tp, t -> new PartitionState()).submitted(offset);
    }

    /** worker 线程: 一条记录处理完成(无论成功、跳过还是解码失败 —— 都算推进) */
    void completed(TopicPartition tp, long offset) {
        PartitionState state = states.get(tp);
        if (state != null) {
            state.completed(offset);
        }
        // state == null: 分区已被撤销(clear), 迟到的完成直接丢弃
    }

    /**
     * 取出各分区「连续完成」推进到的位点(下次应消费的 offset, exclusive),
     * 并重置增量 —— 没有新进度的分区不出现。
     */
    Map<TopicPartition, OffsetAndMetadata> drainCommittable() {
        Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
        states.forEach((tp, state) -> {
            long next = state.advanceAndDrain();
            if (next > 0) {
                result.put(tp, new OffsetAndMetadata(next));
            }
        });
        return result;
    }

    /**
     * 分区被撤销(rebalance)时丢弃状态: 迟到的完成报文与旧位点一并作废。
     */
    void clear(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            states.remove(tp);
        }
    }

    List<TopicPartition> tracked() {
        return new ArrayList<>(states.keySet());
    }

    private static final class PartitionState {
        /** 下一个「连续完成」应到达的 offset(即当前可提交位点) */
        private long next = -1;
        /** 已完成但尚未连成片的 offset(正常情况下同分区按序完成, 堆是防御性的) */
        private final PriorityQueue<Long> completed = new PriorityQueue<>();
        /** drain 后是否又有新进度(决定本次 drain 是否报告该分区) */
        private boolean progressed;
        private boolean started;

        synchronized void submitted(long offset) {
            if (!started) {
                started = true;
                next = offset;
            }
            // 同分区 offset 单调递增, next 已由首个 submitted 定位, 无需调整
        }

        synchronized void completed(long offset) {
            if (!started || offset < next) {
                // 早于当前位点的完成报文只可能来自被撤销分区重新分配后的上一轮残留
                return;
            }
            completed.add(offset);
        }

        /** @return 连续完成推进到的新位点; 自上次 drain 后无新进度返回 -1 */
        synchronized long advanceAndDrain() {
            while (!completed.isEmpty() && completed.peek() == next) {
                completed.poll();
                next++;
                progressed = true;
            }
            if (!progressed) {
                return -1;
            }
            progressed = false;
            return next;
        }
    }
}
