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
package com.noorq.casser.mapping.value;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.noorq.casser.mapping.CasserEntity;
import com.noorq.casser.mapping.CasserProperty;
import com.noorq.casser.support.CasserMappingException;

public final class ValueProviderMap implements Map<String, Object> {

	private final Object source;
	private final ColumnValueProvider valueProvider;
	private final CasserEntity entity;
	
	public ValueProviderMap(Object source, ColumnValueProvider valueProvider, CasserEntity entity) {
		this.source = source;
		this.valueProvider = valueProvider;
		this.entity = entity;
	}
	
	@Override
	public Object get(Object key) {
		if (key instanceof String) {
			String name = (String) key;
			CasserProperty prop = entity.getProperty(name);
			if (prop != null) {
				return valueProvider.getColumnValue(source, -1, prop);
			}
		}
		return null;
	}
	
	@Override
	public Set<String> keySet() {
		return entity.getOrderedProperties().stream().map(p -> p.getPropertyName()).collect(Collectors.toSet());
	}
	
	@Override
	public int size() {
		return entity.getOrderedProperties().size();
	}

	@Override
	public boolean isEmpty() {
		return entity.getOrderedProperties().size() > 0;
	}

	@Override
	public boolean containsKey(Object key) {
		if (key instanceof Object) {
			String s = (String) key;
			return keySet().contains(s);
		}
		return false;
	}

	@Override
	public boolean containsValue(Object value) {
		throwShouldNeverCall();
		return false;
	}

	@Override
	public Object put(String key, Object value) {
		throwShouldNeverCall();
		return null;
	}

	@Override
	public Object remove(Object key) {
		throwShouldNeverCall();
		return null;
	}

	@Override
	public void putAll(Map<? extends String, ? extends Object> m) {
		throwShouldNeverCall();
	}

	@Override
	public void clear() {
		throwShouldNeverCall();
	}

	@Override
	public Collection<Object> values() {
		throwShouldNeverCall();
		return null;
	}

	@Override
	public Set<java.util.Map.Entry<String, Object>> entrySet() {
		throwShouldNeverCall();
		return null;
	}

	private void throwShouldNeverCall() {
		throw new CasserMappingException("should never be called");
	}

	@Override
	public String toString() {
		return source.toString();
	}
	
}
