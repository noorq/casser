/*
 *      Copyright (C) 2015 Noorq, Inc.
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
package com.noorq.casser.support;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public final class Transformers {

	private Transformers() {
	}

	public static <I, O> Set<O> transformSet(Set<I> inputSet, Function<I, O> func) {
		Set<O> set = Sets.newHashSet();
		for (I in : inputSet) {
			set.add(func.apply(in));
		}
		return set;
	}
	
	public static <I, O> List<O> transformList(List<I> inputList, Function<I, O> func) {
		List<O> list = Lists.newArrayList();
		for (I in : inputList) {
			list.add(func.apply(in));
		}
		return list;
	}
	
	public static <I, O, V> Map<O, V> transformMapKey(Map<I, V> inputMap, Function<I, O> func) {
		Map<O, V> map = Maps.newHashMap();
		for (Map.Entry<I, V> e : inputMap.entrySet()) {
			map.put(func.apply(e.getKey()), e.getValue());
		}
		return map;
	}

	public static <I, O, K> Map<K, O> transformMapValue(Map<K, I> inputMap, Function<I, O> func) {
		return Maps.transformValues(inputMap, func::apply);
	}
	
	public static <X, Y, K, V> Map<X, Y> transformMap(Map<K, V> inputMap, Function<K, X> funcKey, Function<V, Y> funcValue) {
		Map<X, Y> map = Maps.newHashMap();
		for (Map.Entry<K, V> e : inputMap.entrySet()) {
			map.put(funcKey.apply(e.getKey()), 
					funcValue.apply(e.getValue()));
		}
		return map;
	}
	
}
