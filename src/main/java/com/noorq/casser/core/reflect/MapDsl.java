/*
 *      Copyright (C) 2015 The Casser Authors
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
package com.noorq.casser.core.reflect;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.noorq.casser.mapping.CasserProperty;
import com.noorq.casser.support.CasserMappingException;
import com.noorq.casser.support.DslPropertyException;

public final class MapDsl<K, V> implements Map<K, V> {

	private final CasserPropertyNode parent;
	
	public MapDsl(CasserPropertyNode parent) {
		this.parent = parent;
	}

	public CasserPropertyNode getParent() {
		return parent;
	}

	@Override
	public V get(Object key) {
		CasserProperty prop = new CasserNamedProperty(key.toString());
		throw new DslPropertyException(new CasserPropertyNode(prop, Optional.of(parent)));
	}
	
	@Override
	public int size() {
		throwShouldNeverCall();
		return 0;
	}

	@Override
	public boolean isEmpty() {
		throwShouldNeverCall();
		return false;
	}

	@Override
	public boolean containsKey(Object key) {
		throwShouldNeverCall();
		return false;
	}

	@Override
	public boolean containsValue(Object value) {
		throwShouldNeverCall();
		return false;
	}

	@Override
	public V put(K key, V value) {
		throwShouldNeverCall();
		return null;
	}

	@Override
	public V remove(Object key) {
		throwShouldNeverCall();
		return null;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		throwShouldNeverCall();
	}

	@Override
	public void clear() {
		throwShouldNeverCall();
	}

	@Override
	public Set<K> keySet() {
		throwShouldNeverCall();
		return null;
	}

	@Override
	public Collection<V> values() {
		throwShouldNeverCall();
		return null;
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		throwShouldNeverCall();
		return null;
	}

	private void throwShouldNeverCall() {
		throw new CasserMappingException("should be never called");
	}

	@Override
	public String toString() {
		return "MapDsl";
	}
	
}
