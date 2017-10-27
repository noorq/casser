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

public class MemcacheDbCache<K, V> implements SessionCache<K, V> {
    //final Cache<K, V> cache;

    MemcacheDbCache() {
        //this.cache = cache;
    }

    @Override
    public void invalidate(K key) {
        //cache.invalidate(key);
    }

    @Override
    public V get(K key) {
        return null;
    }

    @Override
    public void put(K key, V value) {
        //cache.put(key, value);
    }

}
