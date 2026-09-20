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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 分区消费位点的推进规则 —— 多线程并行消费的安全边界全在这里:
 * 只有「从提交点开始连续完成」的 offset 才能提交, 乱序完成不得推进位点。
 */
class PartitionProgressTrackerTest {

    private static final TopicPartition P0 = new TopicPartition("jmqtt-cluster", 0);
    private static final TopicPartition P1 = new TopicPartition("jmqtt-cluster", 1);

    @Test
    @DisplayName("顺序完成: 位点随完成推进, 无新进度时 drain 为空")
    void sequentialCompletionAdvances() {
        PartitionProgressTracker tracker = new PartitionProgressTracker();
        tracker.submitted(P0, 0);
        tracker.submitted(P0, 1);
        tracker.submitted(P0, 2);

        tracker.completed(P0, 0);
        assertEquals(Map.of(P0, new OffsetAndMetadata(1L)), tracker.drainCommittable());

        tracker.completed(P0, 1);
        tracker.completed(P0, 2);
        assertEquals(Map.of(P0, new OffsetAndMetadata(3L)), tracker.drainCommittable());

        assertTrue(tracker.drainCommittable().isEmpty(), "无新进度不得重复提交");
    }

    @Test
    @DisplayName("乱序完成: 高位 offset 先完成不得推进位点, 补齐低位后一次推进到位")
    void outOfOrderCompletionBlocksUntilContiguous() {
        PartitionProgressTracker tracker = new PartitionProgressTracker();
        tracker.submitted(P0, 0);
        tracker.submitted(P0, 1);

        tracker.completed(P0, 1);
        assertTrue(tracker.drainCommittable().isEmpty(), "offset 0 未完成, 不得提交 1");

        tracker.completed(P0, 0);
        assertEquals(Map.of(P0, new OffsetAndMetadata(2L)), tracker.drainCommittable(),
                "低位补齐后应一次推进到连续完成的最大位置");
    }

    @Test
    @DisplayName("多分区互不影响")
    void partitionsAdvanceIndependently() {
        PartitionProgressTracker tracker = new PartitionProgressTracker();
        tracker.submitted(P0, 0);
        tracker.submitted(P1, 0);

        tracker.completed(P0, 0);
        tracker.completed(P1, 0);

        assertEquals(Map.of(P0, new OffsetAndMetadata(1L), P1, new OffsetAndMetadata(1L)),
                tracker.drainCommittable());
    }

    @Test
    @DisplayName("分区被撤销(clear)后: 迟到的完成与位点都被忽略, 重新分配后从新位点开始")
    void clearedPartitionIgnoresLateCompletions() {
        PartitionProgressTracker tracker = new PartitionProgressTracker();
        tracker.submitted(P0, 0);
        tracker.completed(P0, 0);
        tracker.clear(java.util.List.of(P0));

        // 撤销后 worker 的迟到完成不得复活状态
        tracker.completed(P0, 1);
        assertTrue(tracker.drainCommittable().isEmpty());

        // 重新分配, 从已提交位点(1)之后继续
        tracker.submitted(P0, 1);
        tracker.completed(P0, 1);
        assertEquals(Map.of(P0, new OffsetAndMetadata(2L)), tracker.drainCommittable());
    }

    @Test
    @DisplayName("stale 完成报文(低于当前位点)被丢弃, 不阻塞后续推进")
    void staleCompletionBelowNextIsDropped() {
        PartitionProgressTracker tracker = new PartitionProgressTracker();
        tracker.submitted(P0, 5);
        tracker.completed(P0, 5);
        tracker.drainCommittable();

        // 上一轮 epoch 的迟到报文(offset 3 < next 6)
        tracker.completed(P0, 3);
        tracker.submitted(P0, 6);
        tracker.completed(P0, 6);
        assertEquals(Map.of(P0, new OffsetAndMetadata(7L)), tracker.drainCommittable(),
                "stale 报文不得挡住位点推进");
    }
}
