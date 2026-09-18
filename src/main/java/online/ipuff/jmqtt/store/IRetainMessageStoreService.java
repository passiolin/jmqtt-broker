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
package online.ipuff.jmqtt.store;

import online.ipuff.jmqtt.message.RetainMessageStore;

import java.util.List;

/**
 * 保留消息(Retain)存储服务。
 *
 */
public interface IRetainMessageStoreService {

    /**
     * 写入保留消息。同一主题重复写入等价于覆盖。
     */
    void put(RetainMessageStore retain);

    /**
     * 按具体主题名取保留消息。
     */
    RetainMessageStore get(String topic);

    /**
     * 删除指定主题的保留消息。
     */
    boolean remove(String topic);

    /**
     * 指定主题是否存在保留消息。
     */
    boolean containsKey(String topic);

    /**
     * 检索命中该过滤器的全部保留消息。
     *
     * <p>这是<b>过滤器驱动</b>的查询: 输入是过滤器, 输出是匹配的具体主题,
     * 与订阅路由的方向相反。
     */
    List<RetainMessageStore> search(String topicFilter);

    /**
     * 保留消息总数。
     */
    int size();
}
