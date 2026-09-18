/**
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
package online.ipuff.jmqtt.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 异步任务线程池。
 *
 * <p>必须显式提供
 * {@link Executor}, 否则 {@code @Async} 会退化为每次调用新建线程。
 *
 * <p>使用场景是「可以丢、不能阻塞调用方」的旁路任务, 例如会话状态的异步回写、
 * 集群消息的异步投递。因此队列满时采用 {@link ThreadPoolExecutor.CallerRunsPolicy}
 * ——宁可让调用方稍慢, 也不静默丢任务。
 */
@Configuration
public class AsyncConfig {

    public static final String MQTT_EXECUTOR = "mqttTaskExecutor";

    public static final String INFLIGHT_FLUSH_EXECUTOR = "inflightFlushExecutor";

    @Bean(name = MQTT_EXECUTOR)
    public Executor mqttTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(8);
        executor.setMaxPoolSize(64);
        executor.setQueueCapacity(10000);
        executor.setKeepAliveSeconds(60);
        executor.setThreadNamePrefix("jmqtt-task-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(10);
        executor.initialize();
        return executor;
    }

    /**
     * 在途消息镜像的刷盘线程池。
     *
     * <p><b>刻意只用一个线程</b>: 不是为了串行化 Redis 操作(Lettuce 本身是异步的),
     * 而是为了让「提交」有序 —— 同一客户端的两份快照不会因为线程调度而倒序落盘。
     * 提交本身不阻塞(异步命令), 因此单线程足够支撑很高的吞吐。
     *
     * <p>队列满时 {@code CallerRunsPolicy} 会让调用方(发布路径)代为执行 ——
     * 这是一种背压: 与其无声丢快照, 不如让上游稍慢一点。
     */
    @Bean(name = INFLIGHT_FLUSH_EXECUTOR)
    public Executor inflightFlushExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(1);
        executor.setQueueCapacity(50_000);
        executor.setThreadNamePrefix("jmqtt-inflight-flush-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(5);
        executor.initialize();
        return executor;
    }
}
