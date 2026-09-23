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
package online.ipuff.jmqtt.redis;

import io.lettuce.core.ScriptOutputType;

import java.util.Map;

/**
 * 本项目用到的 Redis 同步命令面(自有窄门面)。
 *
 * <p>存在的原因: Lettuce 的单机与集群同步接口({@code RedisCommands} 与
 * {@code RedisAdvancedClusterCommands})是<b>平行层次</b> —— 各自继承同一组命令
 * 混入接口, 却互不为子类型, 也没有公共命令父接口。Java 的接口又是名义类型,
 * 「两边都实现了这些方法」不等于「可以统一赋值」。因此定义自己的门面,
 * 由两个薄适配器分别委托到单机/集群命令对象 —— 模式差异被关在适配器里,
 * 连接管理层与全部业务代码只看到这一份契约。
 *
 * <p>只声明<b>实际用到</b>的命令: 新增用法时在这里显式加方法,
 * 面越窄越清楚, 也不会悄悄依赖起 lettuce 的接口层次。
 */
public interface RedisSyncCommands {

    String ping();

    Long del(String... keys);

    Boolean expire(String key, long seconds);

    Long hdel(String key, String... fields);

    String hget(String key, String field);

    Map<String, String> hgetall(String key);

    Boolean hset(String key, String field, String value);

    Long hset(String key, Map<String, String> map);

    Long sadd(String key, String... members);

    Long llen(String key);

    Long lpush(String key, String... values);

    String ltrim(String key, long start, long stop);

    String rpop(String key);

    <T> T eval(String script, ScriptOutputType type, String[] keys, String... values);
}
