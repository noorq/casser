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
package com.noorq.casser.mapping.map;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.Row;
import com.noorq.casser.mapping.CasserMappingEntity;
import com.noorq.casser.mapping.CasserMappingProperty;
import com.noorq.casser.mapping.value.ColumnValueProvider;
import com.noorq.casser.support.CasserMappingException;

public final class RowProviderMap implements Map<String, Object> {

	private final Row row;
	private final ColumnValueProvider valueProvider;
	private final CasserMappingEntity entity;
	
	public RowProviderMap(Row row, ColumnValueProvider valueProvider, CasserMappingEntity entity) {
		this.row = row;
		this.valueProvider = valueProvider;
		this.entity = entity;
	}
	
	@Override
	public int size() {
		return row.getColumnDefinitions().size();
	}

	@Override
	public boolean isEmpty() {
		return row.getColumnDefinitions().size() == 0;
	}

	@Override
	public boolean containsKey(Object key) {
		if (key instanceof String) {
			String name = (String) key;
			CasserMappingProperty prop = entity.getMappingProperty(name);
			if (prop != null) {
				return row.getColumnDefinitions().contains(prop.getColumnName().getName());
			}
		}
		return false;
	}

	@Override
	public Object get(Object key) {
		if (key instanceof String) {
			String name = (String) key;
			CasserMappingProperty prop = entity.getMappingProperty(name);
			if (prop != null) {
				return valueProvider.getColumnValue(row, -1, prop);
			}
		}
		return null;
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
	public Set<String> keySet() {
		throwShouldNeverCall();
		return null;
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
		return row.toString();
	}

}
