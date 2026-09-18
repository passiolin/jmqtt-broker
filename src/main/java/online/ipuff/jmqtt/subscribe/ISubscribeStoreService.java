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
package online.ipuff.jmqtt.subscribe;

import online.ipuff.jmqtt.message.SubscribeStore;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * 订阅存储服务。
 *
 * <ul>
 *   <li>{@code put(topicFilter, store)} 中的 topicFilter 与
 *       {@code store.topicFilter()} 重复, 去掉入参。</li>
 *   <li>{@code remove(topicFilter, clientId)} 调整为 {@code remove(clientId, topicFilter)},
 *       与其他方法的主键顺序一致。</li>
 * </ul>
 *
 * <p><b>订阅关系只存在于本节点内存, 不做任何复制。</b>这是本架构的核心前提:
 * 节点宕机不会留下任何需要清理的残留状态, 重连也不会产生任何跨节点写入。
 */
public interface ISubscribeStoreService {

    /**
     * 写入订阅。同一 clientId 重复订阅同一过滤器等价于覆盖(幂等)。
     */
    void put(SubscribeStore subscribeStore);

    /**
     * 删除指定客户端的单个订阅。
     */
    boolean remove(String clientId, String topicFilter);

    /**
     * 删除指定客户端的全部订阅。
     *
     * @return 删除条数
     */
    int removeForClient(String clientId);

    /**
     * 检索命中该主题的订阅者。
     *
     * <p>同一客户端有多条过滤器命中时, 只返回一条, 取其中最大的 QoS
     * (MQTT 规范要求按最大 QoS 投递)。
     */
    List<SubscribeStore> search(String topic);

    /**
     * 指定客户端的全部订阅。
     */
    Collection<SubscribeStore> subscriptionsOf(String clientId);

    /**
     * 订阅关系总数。
     */
    int subscriptionCount();

    /**
     * 主题树节点总数, 用于观测剪枝效果。
     */
    int topicNodeCount();

    /**
     * 订阅变更版本号, 每次 {@code put} / {@code remove} / {@code removeForClient} 递增。
     *
     * <p>管理面靠它判断「过滤器视图是否需要重算」。之所以让调用方来问、而不是变更时主动推送,
     * 是因为主动推送在同时发生上万次变更时会把下游淹没, 而下游真正需要的只是
     * 「现在跟刚才不一样了」这一个比特。版本号天然完成这种合并, 也不会漏通知。
     */
    long mutationVersion();

    /**
     * 过滤器直方图: 主题过滤器 -&gt; 订阅该过滤器的客户端数。
     *
     * <p><b>管理面使用, 不要在投递路径上调用</b> —— 它需要遍历整棵主题树,
     * 复杂度是 O(树节点数) 而不是 O(主题层数)。
     *
     * <p>从主题树重算而不是维护增量计数, 是为了让「数值不对」这件事在结构上不可能发生:
     * 增量计数会遇到「同一批订阅被写两次」的场景(会话从持久层恢复时会把订阅重新写回本地树),
     * 一旦漂移就无法自愈; 重算则每次都是真值。
     */
    Map<String, Integer> filterHistogram();
}
