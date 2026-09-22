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

import online.ipuff.jmqtt.TestBrokerProperties;
import online.ipuff.jmqtt.config.BrokerProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并行消费的生命周期冒烟: worker 池按 consumer-threads 创建、
 * 指标可见、优雅停止不挂起。
 *
 * <p>没有真实 Kafka 时 poll 只是空转(bootstrap 连不上会重试),
 * 因此 start/stop 全流程可以在无 broker 的环境下验证 ——
 * 这正是「消费循环异常退出不能挂住 broker 关闭」要钉住的场景。
 * 记录的并行分派与位点推进由 {@link PartitionProgressTrackerTest}
 * 与 {@link KafkaClusterBusKeyTest} 分别钉住。
 */
class KafkaClusterBusLifecycleTest {

    private static BrokerProperties props(int consumerThreads) {
        return TestBrokerProperties.create("node-1", 32, 1000,
                new BrokerProperties.KafkaProperties(
                        true, "127.0.0.1:19092", "jmqtt", "jmqtt-cluster", null,
                        true, List.of(), "", List.of(), false, 1000, 1000, 200,
                        consumerThreads, "none", "latest", "topic", null, null, null));
    }

    @Test
    @DisplayName("consumer-threads=3: worker 池创建、指标可见、stop 优雅退出不挂起")
    void parallelWorkersLifecycle() throws InterruptedException {
        // 基线快照: 其他测试(如缓存的 Spring 上下文)可能遗留同名线程, 只断言本次新增的消失
        java.util.Set<Thread> baseline = workerThreads();
        KafkaClusterBus bus = new KafkaClusterBus(props(3),
                null, (clientId, fromNodeId) -> { });
        try {
            bus.start();
            assertTrue(bus.isRunning(), "无 broker 时总线仍应启动(连接惰性)");
            assertTrue(bus.stats().get("consumerThreads") == 3L,
                    "指标应暴露实际并行度: " + bus.stats().get("consumerThreads"));
        } finally {
            long start = System.nanoTime();
            bus.stop();
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            assertFalse(bus.isRunning());
            assertTrue(elapsedMs < 15_000, "停止必须收敛(worker 排干 + 客户端关闭), 实际 " + elapsedMs + "ms");
        }
        // 本 bus 的 worker 应已退出: 与基线求差, 有界等待消除线程收尾的毫秒级竞态
        long deadline = System.currentTimeMillis() + 3000;
        boolean workersGone = false;
        while (!(workersGone = workerThreads().stream().noneMatch(t -> !baseline.contains(t)))) {
            if (System.currentTimeMillis() > deadline) {
                break;
            }
            Thread.sleep(50);
        }
        assertTrue(workersGone, "停止后不得残留本次启动的 worker 线程" + dumpWorkers());
    }

    private static java.util.Set<Thread> workerThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.getName().startsWith("jmqtt-kafka-worker-"))
                .collect(java.util.stream.Collectors.toSet());
    }

    private static String dumpWorkers() {
        StringBuilder sb = new StringBuilder("\n== 残留 worker 线程栈 ==\n");
        Thread.getAllStackTraces().forEach((t, stack) -> {
            if (t.getName().startsWith("jmqtt-kafka-worker-")) {
                sb.append("-- ").append(t.getName()).append(" state=").append(t.getState()).append('\n');
                for (StackTraceElement e : stack) {
                    sb.append("      at ").append(e).append('\n');
                }
            }
        });
        return sb.toString();
    }

    @Test
    @DisplayName("consumer-threads=1: 与串行消费等价的默认形态同样能启停")
    void singleWorkerLifecycle() {
        KafkaClusterBus bus = new KafkaClusterBus(props(1),
                null, (clientId, fromNodeId) -> { });
        bus.start();
        assertTrue(bus.isRunning());
        assertTrue(bus.stats().get("consumerThreads") == 1L);
        bus.stop();
        assertFalse(bus.isRunning());
    }
}
