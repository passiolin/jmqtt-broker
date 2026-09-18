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
 * 驱逐任务的状态。
 *
 * <p>状态集合刻意把「没能执行」拆成四种, 因为它们对操作者的含义完全不同:
 * <ul>
 *   <li>{@link #REJECTED} —— 节点收到了, 但拒绝执行(已有任务在跑 / 参数非法)。<b>重试有意义</b>。</li>
 *   <li>{@link #EXPIRED} —— 命令签发太久才被取到, 已失去时效。<b>必须重新下发</b>,
 *       而不是重试同一条命令。</li>
 *   <li>{@link #FAILED} —— 执行中途出错。<b>需要看日志</b>。</li>
 *   <li>{@link #ABORTED} —— 人工中止。已断开的部分不会回滚, 也不需要回滚:
 *       会话与订阅都还在, 客户端会自己重连回来。</li>
 * </ul>
 * 把这四种混成一个「失败」, 操作者就无法判断下一步该做什么。
 */
public enum EvictionState {

    /** 命令已取到, 尚未开始执行 */
    RECEIVED,

    /** 正在分批断开 */
    RUNNING,

    /** 计划内的数量已全部断开 */
    COMPLETED,

    /** 被人工中止 */
    ABORTED,

    /** 节点拒绝执行(已有任务在跑, 或参数非法) */
    REJECTED,

    /** 命令超过最大年龄, 未执行 */
    EXPIRED,

    /** 执行过程中出错 */
    FAILED;

    /** 是否已经是终态。控制台据此停止轮询。 */
    public boolean terminal() {
        return this != RECEIVED && this != RUNNING;
    }
}
