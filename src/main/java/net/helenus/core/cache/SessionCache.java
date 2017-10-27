/*
 *      Copyright (C) 2015 The Helenus Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package net.helenus.core.cache;

import com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

public interface SessionCache<K, V> {

    static <K, V> SessionCache<K, V> defaultCache() {
        int MAX_CACHE_SIZE = 10000;
        int MAX_CACHE_EXPIRE_SECONDS = 600;
        return new GuavaCache<K, V>(CacheBuilder.newBuilder().maximumSize(MAX_CACHE_SIZE)
                .expireAfterAccess(MAX_CACHE_EXPIRE_SECONDS, TimeUnit.SECONDS)
                .expireAfterWrite(MAX_CACHE_EXPIRE_SECONDS, TimeUnit.SECONDS).recordStats().build());
    }

    void invalidate(K key);
    V get(K key);
    void put(K key, V value);
}
