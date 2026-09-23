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
package online.ipuff.jmqtt;

import org.springframework.beans.factory.ObjectProvider;

/**
 * 测试用的空 ObjectProvider(无 Redis 场景)。
 * 与各测试文件里手写的匿名类等价, 收敛成一个工具避免重复。
 */
public final class TestObjectProviders {

    private TestObjectProviders() {
    }

    public static <T> ObjectProvider<T> empty() {
        return new ObjectProvider<>() {
            @Override
            public T getObject() {
                throw new java.util.NoSuchElementException();
            }

            @Override
            public T getObject(Object... args) {
                throw new java.util.NoSuchElementException();
            }

            @Override
            public T getIfAvailable() {
                return null;
            }

            @Override
            public T getIfUnique() {
                return null;
            }
        };
    }
}
