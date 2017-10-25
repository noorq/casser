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
package net.helenus.core.operation;

import java.util.*;
import java.util.function.Function;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.Assignment;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

import net.helenus.core.*;
import net.helenus.core.cache.Facet;
import net.helenus.core.reflect.HelenusPropertyNode;
import net.helenus.mapping.HelenusEntity;
import net.helenus.mapping.HelenusProperty;
import net.helenus.mapping.MappingUtil;
import net.helenus.support.HelenusMappingException;
import net.helenus.support.Immutables;

public final class UpdateOperation<E> extends AbstractFilterOperation<E, UpdateOperation<E>> {

	private final List<Assignment> assignments = new ArrayList<Assignment>();
	private final AbstractEntityDraft<E> draft;
	private final Map<String, Object> draftMap;
	private HelenusEntity entity = null;
	private int[] ttl;
	private long[] timestamp;

	public UpdateOperation(AbstractSessionOperations sessionOperations) {
		super(sessionOperations);
		this.draft = null;
		this.draftMap = null;
	}

	public UpdateOperation(AbstractSessionOperations sessionOperations, AbstractEntityDraft<E> draft) {
		super(sessionOperations);
		this.draft = draft;
		this.draftMap = draft.toMap();
	}

	public UpdateOperation(AbstractSessionOperations sessionOperations, HelenusPropertyNode p, Object v) {
		super(sessionOperations);
		this.draft = null;
		this.draftMap = null;

		Object value = sessionOps.getValuePreparer().prepareColumnValue(v, p.getProperty());
		assignments.add(QueryBuilder.set(p.getColumnName(), value));

		addPropertyNode(p);
	}

	public <V> UpdateOperation<E> set(Getter<V> getter, V v) {
		Objects.requireNonNull(getter, "getter is empty");

		HelenusPropertyNode p = MappingUtil.resolveMappingProperty(getter);

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

	public <V> UpdateOperation<E> increment(Getter<V> counterGetter) {
		return increment(counterGetter, 1L);
	}

	public <V> UpdateOperation<E> increment(Getter<V> counterGetter, long delta) {

		Objects.requireNonNull(counterGetter, "counterGetter is empty");

		HelenusPropertyNode p = MappingUtil.resolveMappingProperty(counterGetter);

		assignments.add(QueryBuilder.incr(p.getColumnName(), delta));

		addPropertyNode(p);

		if (draft != null) {
			String key = p.getProperty().getPropertyName();
			draftMap.put(key, (Long) draftMap.get(key) + delta);
		}

		return this;
	}

	public <V> UpdateOperation<E> decrement(Getter<V> counterGetter) {
		return decrement(counterGetter, 1L);
	}

	public <V> UpdateOperation<E> decrement(Getter<V> counterGetter, long delta) {

		Objects.requireNonNull(counterGetter, "counterGetter is empty");

		HelenusPropertyNode p = MappingUtil.resolveMappingProperty(counterGetter);

		assignments.add(QueryBuilder.decr(p.getColumnName(), delta));

		addPropertyNode(p);

		if (draft != null) {
			String key = p.getProperty().getPropertyName();
			draftMap.put(key, (Long) draftMap.get(key) - delta);
		}

		return this;
	}

	/*
	 *
	 *
	 * LIST
	 *
	 */

	public <V> UpdateOperation<E> prepend(Getter<List<V>> listGetter, V value) {

		Objects.requireNonNull(listGetter, "listGetter is empty");
		Objects.requireNonNull(value, "value is empty");

		HelenusPropertyNode p = MappingUtil.resolveMappingProperty(listGetter);
		Object valueObj = prepareSingleListValue(p, value);

		assignments.add(QueryBuilder.prepend(p.getColumnName(), valueObj));

		addPropertyNode(p);

		if (draft != null) {
			String key = p.getProperty().getPropertyName();
			List<V> list = (List<V>) draftMap.get(key);
			list.add(0, value);
		}

		return this;
	}

	public <V> UpdateOperation<E> prependAll(Getter<List<V>> listGetter, List<V> value) {

		Objects.requireNonNull(listGetter, "listGetter is empty");
		Objects.requireNonNull(value, "value is empty");

		HelenusPropertyNode p = MappingUtil.resolveMappingProperty(listGetter);
		List valueObj = prepareListValue(p, value);

		assignments.add(QueryBuilder.prependAll(p.getColumnName(), valueObj));

		addPropertyNode(p);

		if (draft != null && value.size() > 0) {
			String key = p.getProperty().getPropertyName();
			List<V> list = (List<V>) draftMap.get(key);
			list.addAll(0, value);
		}

		return this;
	}

	public <V> UpdateOperation<E> setIdx(Getter<List<V>> listGetter, int idx, V value) {

		Objects.requireNonNull(listGetter, "listGetter is empty");
		Objects.requireNonNull(value, "value is empty");

		HelenusPropertyNode p = MappingUtil.resolveMappingProperty(listGetter);
		Object valueObj = prepareSingleListValue(p, value);

		assignments.add(QueryBuilder.setIdx(p.getColumnName(), idx, valueObj));

		addPropertyNode(p);

		if (draft != null) {
			String key = p.getProperty().getPropertyName();
			List<V> list = (List<V>) draftMap.get(key);
			if (idx < 0) {
				list.add(0, value);
			} else if (idx > list.size()) {
				list.add(list.size(), value);
			} else {
				list.add(idx, value);
			}
			list.add(0, value);
		}

		return this;
	}

	public <V> UpdateOperation<E> append(Getter<List<V>> listGetter, V value) {

		Objects.requireNonNull(listGetter, "listGetter is empty");
		Objects.requireNonNull(value, "value is empty");

		HelenusPropertyNode p = MappingUtil.resolveMappingProperty(listGetter);
		Object valueObj = prepareSingleListValue(p, value);

		assignments.add(QueryBuilder.append(p.getColumnName(), valueObj));

		addPropertyNode(p);

		if (draft != null) {
			String key = p.getProperty().getPropertyName();
			List<V> list = (List<V>) draftMap.get(key);
			list.add(value);
		}

		return this;
	}

	public <V> UpdateOperation<E> appendAll(Getter<List<V>> listGetter, List<V> value) {

		Objects.requireNonNull(listGetter, "listGetter is empty");
		Objects.requireNonNull(value, "value is empty");

		HelenusPropertyNode p = MappingUtil.resolveMappingProperty(listGetter);
		List valueObj = prepareListValue(p, value);

		assignments.add(QueryBuilder.appendAll(p.getColumnName(), valueObj));

		addPropertyNode(p);

		if (draft != null && value.size() > 0) {
			String key = p.getProperty().getPropertyName();
			List<V> list = (List<V>) draftMap.get(key);
			list.addAll(value);
		}

		return this;
	}

	public <V> UpdateOperation<E> discard(Getter<List<V>> listGetter, V value) {

		Objects.requireNonNull(listGetter, "listGetter is empty");
		Objects.requireNonNull(value, "value is empty");

		HelenusPropertyNode p = MappingUtil.resolveMappingProperty(listGetter);
		Object valueObj = prepareSingleListValue(p, value);

		assignments.add(QueryBuilder.discard(p.getColumnName(), valueObj));

		addPropertyNode(p);

		if (draft != null) {
			String key = p.getProperty().getPropertyName();
			List<V> list = (List<V>) draftMap.get(key);
			list.remove(value);
		}

		return this;
	}

	public <V> UpdateOperation<E> discardAll(Getter<List<V>> listGetter, List<V> value) {

		Objects.requireNonNull(listGetter, "listGetter is empty");
		Objects.requireNonNull(value, "value is empty");

		HelenusPropertyNode p = MappingUtil.resolveMappingProperty(listGetter);
		List valueObj = prepareListValue(p, value);

		assignments.add(QueryBuilder.discardAll(p.getColumnName(), valueObj));

		addPropertyNode(p);

		if (draft != null) {
			String key = p.getProperty().getPropertyName();
			List<V> list = (List<V>) draftMap.get(key);
			list.removeAll(value);
		}

		return this;
	}

	private Object prepareSingleListValue(HelenusPropertyNode p, Object value) {
		HelenusProperty prop = p.getProperty();

		Object valueObj = value;

		Optional<Function<Object, Object>> converter = prop.getWriteConverter(sessionOps.getSessionRepository());
		if (converter.isPresent()) {
			List convertedList = (List) converter.get().apply(Immutables.listOf(value));
			valueObj = convertedList.get(0);
		}

		return valueObj;
	}

	private List prepareListValue(HelenusPropertyNode p, List value) {

		HelenusProperty prop = p.getProperty();

		List valueObj = value;

		Optional<Function<Object, Object>> converter = prop.getWriteConverter(sessionOps.getSessionRepository());
		if (converter.isPresent()) {
			valueObj = (List) converter.get().apply(value);
		}

		return valueObj;
	}

	/*
	 *
	 *
	 * SET
	 *
	 *
	 */

	public <V> UpdateOperation<E> add(Getter<Set<V>> setGetter, V value) {

		Objects.requireNonNull(setGetter, "setGetter is empty");
		Objects.requireNonNull(value, "value is empty");

		HelenusPropertyNode p = MappingUtil.resolveMappingProperty(setGetter);
		Object valueObj = prepareSingleSetValue(p, value);

		assignments.add(QueryBuilder.add(p.getColumnName(), valueObj));

		addPropertyNode(p);

		if (draft != null) {
			String key = p.getProperty().getPropertyName();
			Set<V> set = (Set<V>) draftMap.get(key);
			set.add(value);
		}

		return this;
	}

	public <V> UpdateOperation<E> addAll(Getter<Set<V>> setGetter, Set<V> value) {

		Objects.requireNonNull(setGetter, "setGetter is empty");
		Objects.requireNonNull(value, "value is empty");

		HelenusPropertyNode p = MappingUtil.resolveMappingProperty(setGetter);
		Set valueObj = prepareSetValue(p, value);

		assignments.add(QueryBuilder.addAll(p.getColumnName(), valueObj));

		addPropertyNode(p);

		if (draft != null) {
			String key = p.getProperty().getPropertyName();
			Set<V> set = (Set<V>) draftMap.get(key);
			set.addAll(value);
		}

		return this;
	}

	public <V> UpdateOperation<E> remove(Getter<Set<V>> setGetter, V value) {

		Objects.requireNonNull(setGetter, "setGetter is empty");
		Objects.requireNonNull(value, "value is empty");

		HelenusPropertyNode p = MappingUtil.resolveMappingProperty(setGetter);
		Object valueObj = prepareSingleSetValue(p, value);

		assignments.add(QueryBuilder.remove(p.getColumnName(), valueObj));

		addPropertyNode(p);

		if (draft != null) {
			String key = p.getProperty().getPropertyName();
			Set<V> set = (Set<V>) draftMap.get(key);
			set.remove(value);
		}

		return this;
	}

	public <V> UpdateOperation<E> removeAll(Getter<Set<V>> setGetter, Set<V> value) {

		Objects.requireNonNull(setGetter, "setGetter is empty");
		Objects.requireNonNull(value, "value is empty");

		HelenusPropertyNode p = MappingUtil.resolveMappingProperty(setGetter);
		Set valueObj = prepareSetValue(p, value);

		assignments.add(QueryBuilder.removeAll(p.getColumnName(), valueObj));

		addPropertyNode(p);

		if (draft != null) {
			String key = p.getProperty().getPropertyName();
			Set<V> set = (Set<V>) draftMap.get(key);
			set.removeAll(value);
		}

		return this;
	}

	private Object prepareSingleSetValue(HelenusPropertyNode p, Object value) {

		HelenusProperty prop = p.getProperty();
		Object valueObj = value;

		Optional<Function<Object, Object>> converter = prop.getWriteConverter(sessionOps.getSessionRepository());
		if (converter.isPresent()) {
			Set convertedSet = (Set) converter.get().apply(Immutables.setOf(value));
			valueObj = convertedSet.iterator().next();
		}

		return valueObj;
	}

	private Set prepareSetValue(HelenusPropertyNode p, Set value) {

		HelenusProperty prop = p.getProperty();
		Set valueObj = value;

		Optional<Function<Object, Object>> converter = prop.getWriteConverter(sessionOps.getSessionRepository());
		if (converter.isPresent()) {
			valueObj = (Set) converter.get().apply(value);
		}

		return valueObj;
	}

	/*
	 *
	 *
	 * MAP
	 *
	 *
	 */

	public <K, V> UpdateOperation<E> put(Getter<Map<K, V>> mapGetter, K key, V value) {

		Objects.requireNonNull(mapGetter, "mapGetter is empty");
		Objects.requireNonNull(key, "key is empty");

		HelenusPropertyNode p = MappingUtil.resolveMappingProperty(mapGetter);
		HelenusProperty prop = p.getProperty();

		Optional<Function<Object, Object>> converter = prop.getWriteConverter(sessionOps.getSessionRepository());
		if (converter.isPresent()) {
			Map<Object, Object> convertedMap = (Map<Object, Object>) converter.get()
					.apply(Immutables.mapOf(key, value));
			for (Map.Entry<Object, Object> e : convertedMap.entrySet()) {
				assignments.add(QueryBuilder.put(p.getColumnName(), e.getKey(), e.getValue()));
			}
		} else {
			assignments.add(QueryBuilder.put(p.getColumnName(), key, value));
		}

		addPropertyNode(p);

		if (draft != null) {
			((Map<K, V>) draftMap.get(prop.getPropertyName())).put(key, value);
		}

		return this;
	}

	public <K, V> UpdateOperation<E> putAll(Getter<Map<K, V>> mapGetter, Map<K, V> map) {

		Objects.requireNonNull(mapGetter, "mapGetter is empty");
		Objects.requireNonNull(map, "map is empty");

		HelenusPropertyNode p = MappingUtil.resolveMappingProperty(mapGetter);
		HelenusProperty prop = p.getProperty();

		Optional<Function<Object, Object>> converter = prop.getWriteConverter(sessionOps.getSessionRepository());
		if (converter.isPresent()) {
			Map convertedMap = (Map) converter.get().apply(map);
			assignments.add(QueryBuilder.putAll(p.getColumnName(), convertedMap));
		} else {
			assignments.add(QueryBuilder.putAll(p.getColumnName(), map));
		}

		addPropertyNode(p);

		if (draft != null) {
			((Map<K, V>) draftMap.get(prop.getPropertyName())).putAll(map);
		}

		return this;
	}

	@Override
	public BuiltStatement buildStatement(boolean cached) {

		if (entity == null) {
			throw new HelenusMappingException("empty update operation");
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
	public E transform(ResultSet resultSet) {
		if (draft != null) {
			return Helenus.map(draft.getEntityClass(), draft.toMap(draftMap));
		} else {
			return (E) resultSet;
		}
	}

	public UpdateOperation<E> usingTtl(int ttl) {
		this.ttl = new int[1];
		this.ttl[0] = ttl;
		return this;
	}

	public UpdateOperation<E> usingTimestamp(long timestamp) {
		this.timestamp = new long[1];
		this.timestamp[0] = timestamp;
		return this;
	}

	private void addPropertyNode(HelenusPropertyNode p) {
		if (entity == null) {
			entity = p.getEntity();
		} else if (entity != p.getEntity()) {
			throw new HelenusMappingException("you can update columns only in single entity "
					+ entity.getMappingInterface() + " or " + p.getEntity().getMappingInterface());
		}
	}

	@Override
	public E sync(UnitOfWork uow) {// throws TimeoutException {
		if (uow == null) {
			return sync();
		}
		E result = super.sync(uow);
		// TODO(gburd): Only drafted entity objects are updated in the cache at this
		// time.
		if (draft != null) {
			updateCache(uow, result, getFacets());
		}
		return result;
	}

	@Override
	public List<Facet> getFacets() {
		if (entity != null) {
			return entity.getFacets();
		} else {
			return new ArrayList<Facet>();
		}
	}

}
