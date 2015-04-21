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
package com.noorq.casser.core.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.querybuilder.Assignment;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.collect.ImmutableSet;
import com.noorq.casser.core.AbstractSessionOperations;
import com.noorq.casser.core.Filter;
import com.noorq.casser.core.Getter;
import com.noorq.casser.core.SessionRepository;
import com.noorq.casser.core.reflect.CasserPropertyNode;
import com.noorq.casser.mapping.CasserEntity;
import com.noorq.casser.mapping.CasserProperty;
import com.noorq.casser.mapping.MappingUtil;
import com.noorq.casser.mapping.convert.EntityToUDTValueConverter;
import com.noorq.casser.mapping.type.AbstractDataType;
import com.noorq.casser.mapping.type.UDTSetDataType;
import com.noorq.casser.support.CasserMappingException;


public final class UpdateOperation extends AbstractFilterOperation<ResultSet, UpdateOperation> {
	
	private CasserEntity entity = null;
	
	private final List<Assignment> assignments = new ArrayList<Assignment>();

	private int[] ttl;
	private long[] timestamp;
	
	public UpdateOperation(AbstractSessionOperations sessionOperations) {
		super(sessionOperations);
	}
	
	public UpdateOperation(AbstractSessionOperations sessionOperations, CasserPropertyNode p, Object v) {
		super(sessionOperations);
		
		Object value = sessionOps.getValuePreparer().prepareColumnValue(v, p.getProperty());
		assignments.add(QueryBuilder.set(p.getColumnName(), value));
		
		addPropertyNode(p);
	}
	
	public <V> UpdateOperation set(Getter<V> getter, V v) {
		Objects.requireNonNull(getter, "getter is empty");

		CasserPropertyNode p = MappingUtil.resolveMappingProperty(getter);
		
		Object value = sessionOps.getValuePreparer().prepareColumnValue(v, p.getProperty());
		assignments.add(QueryBuilder.set(p.getColumnName(), value));
		
		addPropertyNode(p);
		
		return this;
	}
	
	
	/*
	 * 
	 * 
	 * COUNTER
	 * 
	 * 
	 */
	
	public <V> UpdateOperation increment(Getter<V> counterGetter) {
		return increment(counterGetter, 1L);
	}

	public <V> UpdateOperation increment(Getter<V> counterGetter, long delta) {
		
		Objects.requireNonNull(counterGetter, "counterGetter is empty");

		CasserPropertyNode p = MappingUtil.resolveMappingProperty(counterGetter);
		
		assignments.add(QueryBuilder.incr(p.getColumnName(), delta));
		
		addPropertyNode(p);
		
		return this;
		
	}

	public <V> UpdateOperation decrement(Getter<V> counterGetter) {
		return decrement(counterGetter, 1L);
	}

	public <V> UpdateOperation decrement(Getter<V> counterGetter, long delta) {
		
		Objects.requireNonNull(counterGetter, "counterGetter is empty");

		CasserPropertyNode p = MappingUtil.resolveMappingProperty(counterGetter);
		
		assignments.add(QueryBuilder.decr(p.getColumnName(), delta));
		
		addPropertyNode(p);
		
		return this;
		
	}
	
	/*
	 * 
	 * 
	 * LIST
	 * 
	 */
	

	public <V> UpdateOperation prepend(Getter<List<V>> listGetter, V value) {
		
		Objects.requireNonNull(listGetter, "listGetter is empty");
		Objects.requireNonNull(value, "value is empty");

		CasserPropertyNode p = MappingUtil.resolveMappingProperty(listGetter);
		//Object valueObj = sessionOps.getValuePreparer().prepareColumnValue(value, p.getProperty());
		
		assignments.add(QueryBuilder.prepend(p.getColumnName(), value));
		
		addPropertyNode(p);
		
		return this;
	}

	public <V> UpdateOperation prependAll(Getter<List<V>> listGetter, List<V> value) {
		
		Objects.requireNonNull(listGetter, "listGetter is empty");
		Objects.requireNonNull(value, "value is empty");

		CasserPropertyNode p = MappingUtil.resolveMappingProperty(listGetter);
		//Object valueObj = sessionOps.getValuePreparer().prepareColumnValue(value, p.getProperty());
		
		assignments.add(QueryBuilder.prependAll(p.getColumnName(), value));
		
		addPropertyNode(p);
		
		return this;
	}
	
	public <V> UpdateOperation setIdx(Getter<List<V>> listGetter, int idx, V value) {
		
		Objects.requireNonNull(listGetter, "listGetter is empty");
		Objects.requireNonNull(value, "value is empty");

		CasserPropertyNode p = MappingUtil.resolveMappingProperty(listGetter);
		//Object valueObj = sessionOps.getValuePreparer().prepareColumnValue(value, p.getProperty());
		
		assignments.add(QueryBuilder.setIdx(p.getColumnName(), idx, value));
		
		addPropertyNode(p);
		
		return this;
	}
	
	public <V> UpdateOperation append(Getter<List<V>> listGetter, V value) {
		
		Objects.requireNonNull(listGetter, "listGetter is empty");
		Objects.requireNonNull(value, "value is empty");

		CasserPropertyNode p = MappingUtil.resolveMappingProperty(listGetter);
		//Object valueObj = sessionOps.getValuePreparer().prepareColumnValue(value, p.getProperty());
		
		assignments.add(QueryBuilder.append(p.getColumnName(), value));
		
		addPropertyNode(p);
		
		return this;
	}
	
	public <V> UpdateOperation appendAll(Getter<List<V>> listGetter, List<V> value) {
		
		Objects.requireNonNull(listGetter, "listGetter is empty");
		Objects.requireNonNull(value, "value is empty");

		CasserPropertyNode p = MappingUtil.resolveMappingProperty(listGetter);
		//Object valueObj = sessionOps.getValuePreparer().prepareColumnValue(value, p.getProperty());
		
		assignments.add(QueryBuilder.appendAll(p.getColumnName(), value));
		
		addPropertyNode(p);
		
		return this;
	}
	
	
	public <V> UpdateOperation discard(Getter<List<V>> listGetter, V value) {
		
		Objects.requireNonNull(listGetter, "listGetter is empty");
		Objects.requireNonNull(value, "value is empty");

		CasserPropertyNode p = MappingUtil.resolveMappingProperty(listGetter);
		//Object valueObj = sessionOps.getValuePreparer().prepareColumnValue(value, p.getProperty());
		
		assignments.add(QueryBuilder.discard(p.getColumnName(), value));
		
		addPropertyNode(p);
		
		return this;
	}
	
	public <V> UpdateOperation discardAll(Getter<List<V>> listGetter, List<V> value) {
		
		Objects.requireNonNull(listGetter, "listGetter is empty");
		Objects.requireNonNull(value, "value is empty");

		CasserPropertyNode p = MappingUtil.resolveMappingProperty(listGetter);
		//Object valueObj = sessionOps.getValuePreparer().prepareColumnValue(value, p.getProperty());
		
		assignments.add(QueryBuilder.discardAll(p.getColumnName(), value));
		
		addPropertyNode(p);
		
		return this;
	}
	
	/*
	 * 
	 * 
	 * SET
	 * 
	 * 
	 */
	
	public <V> UpdateOperation add(Getter<Set<V>> setGetter, V value) {
		
		Objects.requireNonNull(setGetter, "setGetter is empty");
		Objects.requireNonNull(value, "value is empty");

		CasserPropertyNode p = MappingUtil.resolveMappingProperty(setGetter);
		CasserProperty prop = p.getProperty();
		
		Object valueObj = value;
		
		Optional<Function<Object, Object>> converter = prop.getWriteConverter(sessionOps.getSessionRepository());
		if (converter.isPresent()) {
			Set convertedSet = (Set) converter.get().apply(ImmutableSet.builder().add(value).build());
			valueObj = convertedSet.iterator().next();
		}

		assignments.add(QueryBuilder.add(p.getColumnName(), valueObj));
		
		addPropertyNode(p);
		return this;
	}
	
	public <V> UpdateOperation addAll(Getter<Set<V>> setGetter, Set<V> value) {
		
		Objects.requireNonNull(setGetter, "setGetter is empty");
		Objects.requireNonNull(value, "value is empty");

		CasserPropertyNode p = MappingUtil.resolveMappingProperty(setGetter);
		CasserProperty prop = p.getProperty();
		
		Set valueObj = value;
		
		Optional<Function<Object, Object>> converter = prop.getWriteConverter(sessionOps.getSessionRepository());
		if (converter.isPresent()) {
			valueObj = (Set) converter.get().apply(value);
		}
		
		assignments.add(QueryBuilder.addAll(p.getColumnName(), valueObj));
		
		addPropertyNode(p);
		return this;
	}
	
	public <V> UpdateOperation remove(Getter<Set<V>> setGetter, V value) {
		
		Objects.requireNonNull(setGetter, "setGetter is empty");
		Objects.requireNonNull(value, "value is empty");

		CasserPropertyNode p = MappingUtil.resolveMappingProperty(setGetter);
		CasserProperty prop = p.getProperty();
		
		Object valueObj = value;
		
		Optional<Function<Object, Object>> converter = prop.getWriteConverter(sessionOps.getSessionRepository());
		if (converter.isPresent()) {
			Set convertedSet = (Set) converter.get().apply(ImmutableSet.builder().add(value).build());
			valueObj = convertedSet.iterator().next();
		}

		assignments.add(QueryBuilder.remove(p.getColumnName(), valueObj));
		
		addPropertyNode(p);
		return this;
	}
	
	public <V> UpdateOperation removeAll(Getter<Set<V>> setGetter, Set<V> value) {
		
		Objects.requireNonNull(setGetter, "setGetter is empty");
		Objects.requireNonNull(value, "value is empty");

		CasserPropertyNode p = MappingUtil.resolveMappingProperty(setGetter);
		CasserProperty prop = p.getProperty();
		
		Set valueObj = value;
		
		Optional<Function<Object, Object>> converter = prop.getWriteConverter(sessionOps.getSessionRepository());
		if (converter.isPresent()) {
			valueObj = (Set) converter.get().apply(value);
		}
		
		assignments.add(QueryBuilder.removeAll(p.getColumnName(), valueObj));
		
		addPropertyNode(p);
		return this;
	}
	
	/*
	 * 
	 * 
	 * MAP
	 * 
	 * 
	 */
	
	public <K,V> UpdateOperation put(Getter<Map<K, V>> mapGetter, K key, V value) {
		
		Objects.requireNonNull(mapGetter, "mapGetter is empty");
		Objects.requireNonNull(key, "key is empty");

		CasserPropertyNode p = MappingUtil.resolveMappingProperty(mapGetter);
		//Object valueObj = sessionOps.getValuePreparer().prepareColumnValue(value, p.getProperty());
		
		assignments.add(QueryBuilder.put(p.getColumnName(), key, value));
		
		addPropertyNode(p);
		
		return this;
    }
	
	public <K,V> UpdateOperation putAll(Getter<Map<K, V>> mapGetter, Map<K, V> map) {
		
		Objects.requireNonNull(mapGetter, "mapGetter is empty");
		Objects.requireNonNull(map, "map is empty");

		CasserPropertyNode p = MappingUtil.resolveMappingProperty(mapGetter);
		//Object valueObj = sessionOps.getValuePreparer().prepareColumnValue(value, p.getProperty());
		
		assignments.add(QueryBuilder.putAll(p.getColumnName(), map));
		
		addPropertyNode(p);
		
		return this;
    }
	
	@Override
	public BuiltStatement buildStatement() {
		
		if (entity == null) {
			throw new CasserMappingException("empty update operation");
		}
		
		Update update = QueryBuilder.update(entity.getName().toCql());

		for (Assignment assignment : assignments) {
			update.with(assignment);
		}

		if (filters != null && !filters.isEmpty()) {
			
			for (Filter<?> filter : filters) {
				update.where(filter.getClause(sessionOps.getValuePreparer()));
			}
		}
		
		if (ifFilters != null && !ifFilters.isEmpty()) {
			
			for (Filter<?> filter : ifFilters) {
				update.onlyIf(filter.getClause(sessionOps.getValuePreparer()));
			}
		}
		
		if (this.ttl != null) {
			update.using(QueryBuilder.ttl(this.ttl[0]));
		}
		if (this.timestamp != null) {
			update.using(QueryBuilder.timestamp(this.timestamp[0]));
		}
		
		return update;
	}

	@Override
	public ResultSet transform(ResultSet resultSet) {
		return resultSet;
	}
	
	public UpdateOperation usingTtl(int ttl) {
		this.ttl = new int[1];
		this.ttl[0] = ttl;
		return this;
	}

	public UpdateOperation usingTimestamp(long timestamp) {
		this.timestamp = new long[1];
		this.timestamp[0] = timestamp;
		return this;
	}
	
	private void addPropertyNode(CasserPropertyNode p) {
		if (entity == null) {
			entity = p.getEntity();
		}
		else if (entity != p.getEntity()) {
			throw new CasserMappingException("you can update columns only in single entity " + entity.getMappingInterface() + " or " + p.getEntity().getMappingInterface());
		}
	}
}
