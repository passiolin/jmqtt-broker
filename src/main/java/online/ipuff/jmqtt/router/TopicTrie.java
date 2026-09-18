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
package online.ipuff.jmqtt.router;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BinaryOperator;

/**
 * MQTT 主题树(前缀树)。
 *
 * <p>路由匹配的成本应当是「主题有多深」的函数, 而不是「订阅有多少条」的函数。
 * 把订阅关系放外部存储、每次发布全量拉取再逐条拼装比对, 复杂度是 O(通配订阅总数)
 * 且每条都带网络往返 —— 连接规模一上来这条路就走不通。
 * 这里用一棵内存前缀树, 匹配降到 O(主题层数), 与订阅总数无关, 且完全不访问外部存储。
 *
 * <h2>两种遍历方向</h2>
 * 同一棵结构承载两种语义完全相反的查询, <b>不能混用同一个方法</b>:
 * <ul>
 *   <li>{@link #matchTopic} —— <b>主题驱动</b>。树里存的是过滤器(含通配符),
 *       输入是具体主题名。用于订阅路由。</li>
 *   <li>{@link #scanByFilter} —— <b>过滤器驱动</b>。树里存的是具体主题名,
 *       输入是过滤器, {@code +} / {@code #} 表现为「枚举子节点」。
 *       用于保留消息索引。</li>
 * </ul>
 *
 * <h2>并发模型</h2>
 * 读路径完全无锁(依赖 {@link ConcurrentHashMap} 的弱一致性迭代, 匹配期间新写入的
 * 订阅最多晚一次投递才生效)。写路径由内部写锁串行化 —— 写只发生在本节点客户端的
 * 订阅变更上, 属于低频操作, 单把锁足够; 同时这也消除了「剪枝删节点」与
 * 「并发插入同一节点」之间的竞态。
 *
 * @param <V> 节点上存放的值类型
 */
public final class TopicTrie<V> {

    private static final class Node<V> {

        private final ConcurrentHashMap<String, Node<V>> children = new ConcurrentHashMap<>();

        /** key -> value。key 在订阅树中是 clientId, 在保留消息树中是主题名。 */
        private final ConcurrentHashMap<String, V> values = new ConcurrentHashMap<>();

        private boolean isEmpty() {
            return children.isEmpty() && values.isEmpty();
        }
    }

    private final Node<V> root = new Node<>();

    /** 写锁: 只保护 insert / remove / clear。读数路径不加锁。 */
    private final ReentrantLock writeLock = new ReentrantLock();

    // ------------------------------------------------------------------
    // 写入
    // ------------------------------------------------------------------

    /**
     * 插入一条记录。
     *
     * @param levels 层级数组, 由 {@link MqttTopic#levels} 切分; 订阅树传入过滤器, 保留树传入主题名
     * @param key    同一节点内的唯一键(订阅树用 clientId), 重复插入同一 key 等价于覆盖
     * @param value  值
     */
    public void insert(String[] levels, String key, V value) {
        writeLock.lock();
        try {
            Node<V> node = root;
            for (String level : levels) {
                node = node.children.computeIfAbsent(level, k -> new Node<>());
            }
            node.values.put(key, value);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 删除一条记录, 并自底向上回收空节点。
     *
     * <p>剪枝解决的是长期运行的内存膨胀: 客户端会反复订阅/退订不同过滤器,
     * 不回收的话树会随历史过滤器单调增长。
     *
     * @return 是否确实删除了内容
     */
    public boolean remove(String[] levels, String key) {
        writeLock.lock();
        try {
            return remove(root, levels, 0, key);
        } finally {
            writeLock.unlock();
        }
    }

    private boolean remove(Node<V> node, String[] levels, int index, String key) {
        if (index == levels.length) {
            return node.values.remove(key) != null;
        }
        Node<V> child = node.children.get(levels[index]);
        if (child == null) {
            return false;
        }
        boolean removed = remove(child, levels, index + 1, key);
        if (child.isEmpty()) {
            // 两参数 remove: 仅当该键仍映射到同一个 child 时才删, 避免误删并发新建的节点
            node.children.remove(levels[index], child);
        }
        return removed;
    }

    public void clear() {
        writeLock.lock();
        try {
            root.children.clear();
            root.values.clear();
        } finally {
            writeLock.unlock();
        }
    }

    // ------------------------------------------------------------------
    // 读: 主题驱动(订阅路由)
    // ------------------------------------------------------------------

    /**
     * 用具体主题名匹配树中的过滤器。用于订阅路由。
     *
     * @param topicLevels 主题层级
     * @param merger      同一 key 被多条过滤器命中时的合并策略(订阅场景传入「取较大 QoS」);
     *                    传 {@code null} 表示后写覆盖
     * @return key(订阅树中为 clientId) -> 命中值
     */
    public Map<String, V> matchTopic(String[] topicLevels, BinaryOperator<V> merger) {
        Map<String, V> result = new HashMap<>();
        collectMatch(root, topicLevels, 0, result, merger);
        return result;
    }

    private void collectMatch(Node<V> node, String[] levels, int index,
                              Map<String, V> out, BinaryOperator<V> merger) {
        if (index == levels.length) {
            // 要点: 剩余路径为空时也必须检查 # 分支 ——
            // 规范要求 sport/tennis/# 匹配父层 sport/tennis 本身
            Node<V> multi = node.children.get(MqttTopic.MULTI_WILDCARD);
            if (multi != null) {
                mergeAll(out, multi.values, merger);
            }
            mergeAll(out, node.values, merger);
            return;
        }

        // $ 隔离: 仅在根层生效。以通配符开头的过滤器不得匹配 $ 开头的主题
        boolean metadataTopic = index == 0 && MqttTopic.isMetadataLevel(levels[0]);

        if (!metadataTopic) {
            // # 匹配剩余所有层, 命中后不再下降(过滤器校验保证 # 只可能在最后一层, 故其节点无子节点)
            Node<V> multi = node.children.get(MqttTopic.MULTI_WILDCARD);
            if (multi != null) {
                mergeAll(out, multi.values, merger);
            }
            // + 分叉: 消耗一层
            Node<V> single = node.children.get(MqttTopic.SINGLE_WILDCARD);
            if (single != null) {
                collectMatch(single, levels, index + 1, out, merger);
            }
        }

        // 精确分支
        Node<V> exact = node.children.get(levels[index]);
        if (exact != null) {
            collectMatch(exact, levels, index + 1, out, merger);
        }
    }

    private void mergeAll(Map<String, V> out, ConcurrentHashMap<String, V> values, BinaryOperator<V> merger) {
        if (values.isEmpty()) {
            return;
        }
        values.forEach((key, value) -> {
            if (merger == null) {
                out.put(key, value);
            } else {
                out.merge(key, value, merger);
            }
        });
    }

    // ------------------------------------------------------------------
    // 读: 过滤器驱动(保留消息索引)
    // ------------------------------------------------------------------

    /**
     * 用过滤器扫描树中的具体主题名。用于保留消息索引。
     *
     * <p><b>注意与 {@link #matchTopic} 的区别</b>: 这里的树存的是具体主题名,
     * 过滤器中的 {@code +} / {@code #} 表现为「枚举子节点」。两个方向不能复用同一实现。
     *
     * @param filterLevels 过滤器层级
     * @return 主题名 -> 值
     */
    public Map<String, V> scanByFilter(String[] filterLevels) {
        Map<String, V> result = new HashMap<>();
        scan(root, filterLevels, 0, result);
        return result;
    }

    private void scan(Node<V> node, String[] filter, int index, Map<String, V> out) {
        if (index == filter.length) {
            out.putAll(node.values);
            return;
        }
        String level = filter[index];

        if (MqttTopic.MULTI_WILDCARD.equals(level)) {
            // # 匹配剩余所有层, 且必须包含"父层本身" ——
            // 与 matchTopic 中的同名要点形成呼应, 是同一条规范的两面
            collectSubtree(node, out, index == 0);
            return;
        }

        if (MqttTopic.SINGLE_WILDCARD.equals(level)) {
            node.children.forEach((childLevel, child) -> {
                // $ 隔离: 根层的 + 不枚举 $ 开头的层
                if (index == 0 && MqttTopic.isMetadataLevel(childLevel)) {
                    return;
                }
                scan(child, filter, index + 1, out);
            });
            return;
        }

        Node<V> child = node.children.get(level);
        if (child != null) {
            scan(child, filter, index + 1, out);
        }
    }

    private void collectSubtree(Node<V> node, Map<String, V> out, boolean skipMetadataChildren) {
        out.putAll(node.values);
        node.children.forEach((level, child) -> {
            if (skipMetadataChildren && MqttTopic.isMetadataLevel(level)) {
                return;
            }
            collectSubtree(child, out, false);
        });
    }

    // ------------------------------------------------------------------
    // 指标
    // ------------------------------------------------------------------

    /**
     * 节点总数。用于观测剪枝是否正常工作(长期运行应当趋于稳定而非单调增长)。
     */
    public int nodeCount() {
        return countNodes(root);
    }

    private int countNodes(Node<V> node) {
        int count = 1;
        for (Node<V> child : node.children.values()) {
            count += countNodes(child);
        }
        return count;
    }

    /**
     * 记录总数。
     */
    public int valueCount() {
        return countValues(root);
    }

    private int countValues(Node<V> node) {
        int count = node.values.size();
        for (Node<V> child : node.children.values()) {
            count += countValues(child);
        }
        return count;
    }

    // ------------------------------------------------------------------
    // 枚举(管理面)
    // ------------------------------------------------------------------

    /**
     * 值遍历回调。
     *
     * @param <V> 值类型
     */
    @FunctionalInterface
    public interface ValueVisitor<V> {

        /**
         * @param pathLevels 该节点的完整层级路径。仅在回调期间有效, 不要持有
         * @param values     该节点上的值(key -&gt; value)。是内部结构的实时视图, 不要持有
         * @return false 表示停止遍历
         */
        boolean visit(String[] pathLevels, Map<String, V> values);
    }

    /**
     * 深度优先遍历, 对每个<b>存有值</b>的节点回调一次。用于管理面枚举订阅与保留消息。
     *
     * <p><b>刻意不返回完整集合。</b>十万级连接下订阅条目可达百万, 一次性物化会直接吃光堆 ——
     * 删掉这一层「先全量再分页」的实现, 是让管理接口不会反过来打垮接入层的关键。
     * 调用方在回调里自行累计并在够用时返回 {@code false} 中止。
     *
     * <p>读路径不加锁, 因此遍历期间新写入的订阅可能被看到、也可能不被看到 ——
     * 对「看一眼当前有哪些 topic」这个用途, 弱一致性足够, 不值得让写操作等一次全树遍历。
     */
    public void forEachValue(ValueVisitor<V> visitor) {
        walk(root, new ArrayDeque<>(), visitor);
    }

    private boolean walk(Node<V> node, Deque<String> path, ValueVisitor<V> visitor) {
        if (!node.values.isEmpty() && !visitor.visit(path.toArray(new String[0]), node.values)) {
            return false;
        }
        for (Map.Entry<String, Node<V>> entry : node.children.entrySet()) {
            path.addLast(entry.getKey());
            boolean keepGoing = walk(entry.getValue(), path, visitor);
            path.removeLast();
            if (!keepGoing) {
                return false;
            }
        }
        return true;
    }
}
