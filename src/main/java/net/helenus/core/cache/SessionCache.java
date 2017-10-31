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

import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface SessionCache<K, V> {

	static final Logger LOG = LoggerFactory.getLogger(SessionCache.class);

	static <K, V> SessionCache<K, V> defaultCache() {
		GuavaCache<K, V> cache;
		RemovalListener<K, V> listener =
				new RemovalListener<K, V>() {
					@Override
					public void onRemoval(RemovalNotification<K, V> n) {
						if (n.wasEvicted()) {
							String cause = n.getCause().name();
							LOG.info(cause);
						}
					}
				};

		cache = new GuavaCache<K, V>(CacheBuilder.newBuilder()
				.maximumSize(25_000)
				.expireAfterAccess(5, TimeUnit.MINUTES)
				.softValues()
				.removalListener(listener)
				.build());

		return cache;
	}

	void invalidate(K key);
	V get(K key);
	void put(K key, V value);
}
