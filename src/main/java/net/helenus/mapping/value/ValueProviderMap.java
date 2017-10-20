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
package net.helenus.mapping.value;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import net.helenus.mapping.HelenusEntity;
import net.helenus.mapping.HelenusProperty;
import net.helenus.support.HelenusMappingException;

public final class ValueProviderMap implements Map<String, Object> {

	private final Object source;
	private final ColumnValueProvider valueProvider;
	private final HelenusEntity entity;

	public ValueProviderMap(Object source, ColumnValueProvider valueProvider, HelenusEntity entity) {
		this.source = source;
		this.valueProvider = valueProvider;
		this.entity = entity;
	}

	@Override
	public Object get(Object key) {
		if (key instanceof String) {
			String name = (String) key;
			HelenusProperty prop = entity.getProperty(name);
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
		throwShouldNeverCall("containsValue()");
		return false;
	}

	@Override
	public Object put(String key, Object value) {
		throwShouldNeverCall("put()");
		return null;
	}

	@Override
	public Object remove(Object key) {
		throwShouldNeverCall("remove()");
		return null;
	}

	@Override
	public void putAll(Map<? extends String, ? extends Object> m) {
		throwShouldNeverCall("putAll()");
	}

	@Override
	public void clear() {
		throwShouldNeverCall("clear()");
	}

	@Override
	public Collection<Object> values() {
		throwShouldNeverCall("values()");
		return null;
	}

	@Override
	public Set<java.util.Map.Entry<String, Object>> entrySet() {
		throwShouldNeverCall("entrySet()");
		return null;
	}

	private void throwShouldNeverCall(String methodName) {
		throw new HelenusMappingException(String.format(
				"the method {} should never be called on an instance of a Helenus ValueProviderMap", methodName));
	}

	@Override
	public String toString() {
		return source.toString();
	}
}
