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
package online.ipuff.jmqtt.authz;

import io.netty.channel.Channel;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * 每连接的串行异步门: ACL 判定挂起期间, 同连接的后续报文<b>按序排队</b>。
 *
 * <p>存在的理由只有一个: MQTT 要求同一连接上的报文处理保持顺序(QoS 1 的
 * PUBACK 顺序、同主题消息顺序)。ACL 判定一旦异步, 后到的报文就可能越过
 * 先到的先被处理 —— 顺序保证直接破损。排队是有界的: 超限关连接,
 * 不能为了等一个卡死的 ACL 服务无限吃内存。
 *
 * <p><b>线程约定</b>: {@link #offer} 与 {@link #release} 都只在所属连接的
 * EventLoop 上调用(异步结果必须 hop 回 EventLoop 再 release), 因此无需加锁。
 *
 * <p>空闲时的开销是一次布尔判断 + 内联执行 —— ACL 关闭或全部缓存命中的
 * 连接不会察觉它的存在。
 */
public final class AclGate {

    /** 排队上限: 每连接最多缓存多少条等 ACL 的报文 */
    static final int MAX_QUEUED = 64;

    private final Deque<Runnable> queued = new ArrayDeque<>();
    private boolean busy;

    /**
     * 提交一个动作。空闲时立即执行; 忙时排队(有界, 超限关连接)。
     * 动作完成自己的异步工作后<b>必须</b>调用 {@link #release} 驱动队列,
     * 否则该连接的后续报文将永久卡住。
     */
    public void offer(Channel channel, Runnable action) {
        if (busy) {
            if (queued.size() >= MAX_QUEUED) {
                channel.close();
                return;
            }
            queued.add(action);
            return;
        }
        busy = true;
        action.run();
    }

    /**
     * 当前动作的异步工作结束(含报文已处理/已拒绝/连接已断)后调用, 驱动下一条。
     */
    public void release() {
        Runnable next = queued.poll();
        if (next == null) {
            busy = false;
            return;
        }
        next.run();
    }
}
