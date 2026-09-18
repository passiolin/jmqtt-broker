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

import online.ipuff.jmqtt.redis.RedisConnectionManager;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 把命令结果与驱逐进度写到 Redis, 供控制台查询。
 *
 * <p>写失败不重试、不抛异常: 进度是<b>观测数据</b>, 不是业务数据。
 * 一次失败的进度上报最多让控制台少看到一帧, 而让它把异常抛回执行线程,
 * 就会把一条本来在正常推进的驱逐任务打断 —— 代价完全不对等。
 */
@Component
public class AdminCommandReporter {

    private final ObjectProvider<RedisConnectionManager> redis;
    private final AdminRedisKeys keys;
    private final AdminProperties properties;

    public AdminCommandReporter(ObjectProvider<RedisConnectionManager> redis,
                                AdminRedisKeys keys,
                                AdminProperties properties) {
        this.redis = redis;
        this.keys = keys;
        this.properties = properties;
    }

    /**
     * 写入/覆盖一条命令的结果。每次都是全量覆盖同名字段, 因此不需要读-改-写。
     */
    public void write(String commandId, Map<String, String> fields) {
        if (commandId == null || fields == null || fields.isEmpty()) {
            return;
        }
        RedisConnectionManager manager = redis.getIfAvailable();
        if (manager == null) {
            return;
        }
        String key = keys.commandResult(commandId);
        int ttl = properties.commandResultTtlSeconds();
        manager.execute("admin.commandResult", () -> {
            manager.commands().hset(key, fields);
            manager.commands().expire(key, ttl);
            return null;
        }, null);
    }

    public void write(String commandId, String state, String message) {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("state", state);
        fields.put("updatedAt", String.valueOf(System.currentTimeMillis()));
        if (message != null) {
            fields.put("message", message);
        }
        write(commandId, fields);
    }

    public void write(EvictionTask task) {
        write(task.taskId(), task.toRedisFields());
    }
}
