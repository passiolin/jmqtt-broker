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
package online.ipuff.jmqtt.router;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 主题树: {@code hasMatch} 与 {@code matchTopic} 的语义等价性,
 * 以及分片写锁下「剪枝 vs 并发插入」的正确性。
 */
class TopicTrieTest {

    private static String[] levels(String topic) {
        return topic.split("/", -1);
    }

    private static void insertAll(TopicTrie<String> trie, String... filters) {
        for (String filter : filters) {
            trie.insert(levels(filter), filter, filter);
        }
    }

    // ------------------------------------------------------------------
    // hasMatch 语义等价性
    // ------------------------------------------------------------------

    @Test
    @DisplayName("hasMatch 恒等于 !matchTopic().isEmpty() —— 精确/+/ #/父层 #/ $ 隔离")
    void hasMatchEqualsMatchTopicEmptiness() {
        TopicTrie<String> trie = new TopicTrie<>();
        insertAll(trie, "sport/tennis/player1",
                "sport/tennis/#",
                "sport/+",
                "#",
                "+/+/player1",
                "$SYS/broker/uptime",
                "sport//x");

        String[] topics = {
                "sport/tennis/player1", "sport/tennis", "sport/tennis/player2",
                "sport", "sport/", "sport//x", "sport//y",
                "finance", "finance/stock/ibm",
                "$SYS/broker/uptime", "$SYS/broker/clients",
                "a/b/player1", "a/b/c/d",
        };
        for (String topic : topics) {
            boolean viaMatch = !trie.matchTopic(levels(topic), null).isEmpty();
            assertEquals(viaMatch, trie.hasMatch(levels(topic)),
                    "主题 [" + topic + "] 两种判法必须一致");
        }
    }

    @Test
    @DisplayName("空树与无命中 —— hasMatch 为 false")
    void hasMatchFalseWhenEmpty() {
        TopicTrie<String> trie = new TopicTrie<>();
        assertFalse(trie.hasMatch(levels("a/b")));

        insertAll(trie, "a/b");
        assertFalse(trie.hasMatch(levels("a/c")), "兄弟主题不命中");
        assertFalse(trie.hasMatch(levels("a/b/c")), "更深主题不命中精确过滤器");
        assertTrue(trie.hasMatch(levels("a/b")));
    }

    @Test
    @DisplayName("删空后 hasMatch 归 false —— 随剪枝一起收敛")
    void hasMatchFalseAfterRemove() {
        TopicTrie<String> trie = new TopicTrie<>();
        trie.insert(levels("a/b"), "c1", "v");
        assertTrue(trie.hasMatch(levels("a/b")));

        trie.remove(levels("a/b"), "c1");
        assertFalse(trie.hasMatch(levels("a/b")), "最后一条删除后不再命中");
        assertEquals(1, trie.nodeCount(), "空节点已剪枝, 只剩根");
    }

    // ------------------------------------------------------------------
    // 分片写锁: 剪枝 vs 并发插入
    // ------------------------------------------------------------------

    @Test
    @DisplayName("并发插同一路径不丢订阅 —— 分片锁串行化同首层写入")
    void concurrentInsertSamePathLosesNothing() throws Exception {
        TopicTrie<String> trie = new TopicTrie<>();
        int threads = 8;
        int perThread = 500;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            int base = t * perThread;
            futures.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < perThread; i++) {
                    trie.insert(levels("a/b/c"), "k" + (base + i), "v");
                }
                return null;
            }));
        }
        start.countDown();
        for (java.util.concurrent.Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();

        assertEquals(threads * perThread, trie.valueCount(), "并发插入一条不丢");
    }

    @Test
    @DisplayName("剪枝与并发插入竞争不产生孤儿节点 —— 复现「摘节点前被塞值」的竞态")
    void pruneRacingInsertDoesNotOrphan() throws InterruptedException {
        TopicTrie<String> trie = new TopicTrie<>();
        int rounds = 2000;
        AtomicBoolean lost = new AtomicBoolean(false);

        // 线程 A: 反复插入/删除 a/b —— 每次删除都触发自底向上剪枝
        // 线程 B: 反复插入/删除 a/b/c —— 目标正是 A 要剪掉的节点
        // 竞态若存在, B 的值会落在已摘除的子树里, valueCount 对不上账
        Thread pruner = new Thread(() -> {
            for (int i = 0; i < rounds; i++) {
                trie.insert(levels("a/b"), "pa", "v");
                trie.remove(levels("a/b"), "pa");
            }
        });
        Thread racer = new Thread(() -> {
            for (int i = 0; i < rounds; i++) {
                trie.insert(levels("a/b/c"), "ra", "v");
                if (!trie.hasMatch(levels("a/b/c"))) {
                    lost.set(true);
                }
                trie.remove(levels("a/b/c"), "ra");
            }
        });
        pruner.start();
        racer.start();
        pruner.join(30_000);
        racer.join(30_000);

        assertFalse(lost.get(), "插入后立即查询必须可见(不许丢进孤儿子树)");
        // 终态: 两条都删净, 树应剪到只剩根; 若残留 a/b 或 a/b/c 说明剪枝被并发插入误伤后
        // 留下了对不上账的结构
        trie.insert(levels("x/y"), "z", "v");
        assertTrue(trie.hasMatch(levels("x/y")));
        trie.remove(levels("x/y"), "z");
        assertEquals(1, trie.nodeCount(), "全部删除后树收敛到只剩根");
    }

    @Test
    @DisplayName("不同首层并发写互不干扰 —— 分片并行的正确性面")
    void concurrentWritesAcrossShardsAreIndependent() throws Exception {
        TopicTrie<String> trie = new TopicTrie<>();
        int threads = 8;
        int perThread = 300;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            int id = t;
            futures.add(pool.submit(() -> {
                start.await();
                String filter = "prefix" + id + "/x/y";
                for (int i = 0; i < perThread; i++) {
                    trie.insert(levels(filter), "k" + i, "v");
                    trie.remove(levels(filter), "k" + i);
                }
                // 收尾插入一条留着, 逐首层校验互不影响
                trie.insert(levels(filter), "keep", "v");
                return null;
            }));
        }
        start.countDown();
        for (java.util.concurrent.Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();

        for (int t = 0; t < threads; t++) {
            assertTrue(trie.hasMatch(levels("prefix" + t + "/x/y")),
                    "prefix" + t + " 的收尾订阅必须存活");
        }
        assertEquals(threads, trie.valueCount(), "每首层恰好一条存活");
    }

    @Test
    @DisplayName("clear 清空整棵分片树 —— 全锁路径无死锁且彻底")
    void clearEmptiesAllShards() throws InterruptedException {
        TopicTrie<String> trie = new TopicTrie<>();
        for (int i = 0; i < 32; i++) {
            trie.insert(levels("p" + i + "/a"), "k", "v");
        }
        assertEquals(32, trie.valueCount());

        // 并发 clear + insert: 不死锁, 且结束后结构一致(要么看到插入, 要么被清掉)
        CountDownLatch done = new CountDownLatch(2);
        new Thread(() -> {
            trie.clear();
            done.countDown();
        }).start();
        new Thread(() -> {
            trie.insert(levels("zz/yy"), "k", "v");
            done.countDown();
        }).start();
        assertTrue(done.await(10, TimeUnit.SECONDS), "clear 与并发插入不得死锁");

        trie.clear();
        assertEquals(0, trie.valueCount());
        assertEquals(1, trie.nodeCount(), "清空后只剩根");
    }
}
