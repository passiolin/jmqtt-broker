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
package online.ipuff.jmqtt.store;

import online.ipuff.jmqtt.TestObjectProviders;
import online.ipuff.jmqtt.message.DupPublishMessageStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * 在途存储与持久镜像的协作方式。
 *
 * <p>镜像用 Mockito 替身: 这里测的不是镜像本身的写入策略
 * (那在 {@link InflightPersistenceTest}), 而是本类<b>何时</b>把变更交给它、
 * 何时该同步删除 —— 特别是 removeByClient 是会话销毁路径,
 * 删除必须立即生效, 不能排进异步刷盘队列等下一个周期。
 */
class DupPublishMessageStoreServiceTest {

    private static final String CLIENT = "device-1";

    private static DupPublishMessageStore message(int messageId) {
        return new DupPublishMessageStore(CLIENT, "a/b", 1, messageId, new byte[0]);
    }

    @Test
    @DisplayName("put/remove 把变化后的完整快照交给镜像")
    void mutationsAreMirrored() {
        InflightPersistence mirror = mock(InflightPersistence.class);
        DupPublishMessageStoreService store = new DupPublishMessageStoreService(TestObjectProviders.of(mirror));

        store.put(message(1));
        verify(mirror).onChanged(eq(CLIENT),
                argThat(snapshot -> snapshot.size() == 1 && snapshot.get(0).messageId() == 1));

        store.put(message(2));
        verify(mirror).onChanged(eq(CLIENT), argThat(snapshot -> snapshot.size() == 2));

        store.remove(CLIENT, 1);
        verify(mirror).onChanged(eq(CLIENT),
                argThat(snapshot -> snapshot.size() == 1 && snapshot.get(0).messageId() == 2));
    }

    @Test
    @DisplayName("removeByClient 同步 clear 镜像, 而不是排一个异步空快照")
    void removeByClientClearsMirrorImmediately() {
        InflightPersistence mirror = mock(InflightPersistence.class);
        DupPublishMessageStoreService store = new DupPublishMessageStoreService(TestObjectProviders.of(mirror));

        store.put(message(1));
        store.removeByClient(CLIENT);

        verify(mirror).clear(CLIENT);
        verify(mirror, never()).onChanged(eq(CLIENT), argThat(List::isEmpty));
    }

    @Test
    @DisplayName("无镜像实例(未启用 Redis)全程安全, 内存语义不受影响")
    void noMirrorInstanceIsSafe() {
        DupPublishMessageStoreService store = new DupPublishMessageStoreService();

        store.put(message(1));
        store.put(message(2));
        assertEquals(2, store.size());
        assertTrue(store.get(CLIENT).stream().anyMatch(m -> m.messageId() == 1),
                "内存里应能读回在途消息");

        store.remove(CLIENT, 1);
        store.removeByClient(CLIENT);
        assertEquals(0, store.size());
    }
}
