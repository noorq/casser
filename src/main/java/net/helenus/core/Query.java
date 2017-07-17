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
package net.helenus.core;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.datastax.driver.core.querybuilder.BindMarker;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import net.helenus.mapping.OrderingDirection;

/**
 * Sugar methods for the queries
 *
 */

public final class Query {

	private Query() {
	}

	public static BindMarker marker() {
		return QueryBuilder.bindMarker();
	}

	public static BindMarker marker(String name) {
		return QueryBuilder.bindMarker(name);
	}

	public static Ordered asc(Getter<?> getter) {
		return new Ordered(getter, OrderingDirection.ASC);
	}

	public static Ordered desc(Getter<?> getter) {
		return new Ordered(getter, OrderingDirection.DESC);
	}

	public static <V> Postulate<V> eq(V val) {
		return Postulate.of(Operator.EQ, val);
	}

	public static <V> Postulate<V> lt(V val) {
		return Postulate.of(Operator.LT, val);
	}

	public static <V> Postulate<V> lte(V val) {
		return Postulate.of(Operator.LTE, val);
	}

	public static <V> Postulate<V> gt(V val) {
		return Postulate.of(Operator.GT, val);
	}

	public static <V> Postulate<V> gte(V val) {
		return Postulate.of(Operator.GTE, val);
	}

	public static <V> Postulate<V> in(V[] vals) {
		return new Postulate<V>(Operator.IN, vals);
	}

	public static <K, V> Getter<V> getIdx(Getter<List<V>> listGetter, int index) {
		Objects.requireNonNull(listGetter, "listGetter is null");

		return new Getter<V>() {

			@Override
			public V get() {
				return listGetter.get().get(index);
			}

		};
	}

	public static <K, V> Getter<V> get(Getter<Map<K, V>> mapGetter, K k) {
		Objects.requireNonNull(mapGetter, "mapGetter is null");
		Objects.requireNonNull(k, "key is null");

		return new Getter<V>() {

			@Override
			public V get() {
				return mapGetter.get().get(k);
			}

		};
	}

}
