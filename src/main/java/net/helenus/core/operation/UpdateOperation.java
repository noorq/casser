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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.Assignment;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import net.helenus.core.*;
import net.helenus.core.cache.BoundFacet;
import net.helenus.core.cache.CacheUtil;
import net.helenus.core.cache.Facet;
import net.helenus.core.reflect.HelenusPropertyNode;
import net.helenus.core.reflect.MapExportable;
import net.helenus.mapping.HelenusEntity;
import net.helenus.mapping.HelenusProperty;
import net.helenus.mapping.MappingUtil;
import net.helenus.mapping.value.BeanColumnValueProvider;
import net.helenus.support.HelenusException;
import net.helenus.support.HelenusMappingException;
import net.helenus.support.Immutables;

public final class UpdateOperation<E> extends AbstractFilterOperation<E, UpdateOperation<E>> {

  private final Map<Assignment, BoundFacet> assignments = new HashMap<>();
  private final AbstractEntityDraft<E> draft;
  private final Map<String, Object> draftMap;
  private HelenusEntity entity = null;
  private Object pojo;
  private int[] ttl;
  private long[] timestamp;
  private long writeTime = 0L;

  public UpdateOperation(AbstractSessionOperations sessionOperations) {
    super(sessionOperations);
    this.draft = null;
    this.draftMap = null;
  }

  public UpdateOperation(
      AbstractSessionOperations sessionOperations, AbstractEntityDraft<E> draft) {
    super(sessionOperations);
    this.draft = draft;
    this.draftMap = draft.toMap();
  }

  public UpdateOperation(AbstractSessionOperations sessionOperations, Object pojo) {
    super(sessionOperations);
    this.draft = null;
    this.draftMap = null;

    if (pojo != null) {
      this.entity = Helenus.resolve(MappingUtil.getMappingInterface(pojo));
      if (this.entity != null && entity.isCacheable() && pojo instanceof MapExportable) {
        this.pojo = pojo;
      }
    }
  }

  public UpdateOperation(
      AbstractSessionOperations sessionOperations, HelenusPropertyNode p, Object v) {
    super(sessionOperations);
    this.draft = null;
    this.draftMap = null;

    Object value = sessionOps.getValuePreparer().prepareColumnValue(v, p.getProperty());
    assignments.put(QueryBuilder.set(p.getColumnName(), value), new BoundFacet(p.getProperty(), v));

    addPropertyNode(p);
  }

  public <V> UpdateOperation<E> set(Getter<V> getter, V v) {
    Objects.requireNonNull(getter, "getter is empty");

    HelenusPropertyNode p = MappingUtil.resolveMappingProperty(getter);
    HelenusProperty prop = p.getProperty();

    Object value = sessionOps.getValuePreparer().prepareColumnValue(v, prop);
    assignments.put(QueryBuilder.set(p.getColumnName(), value), new BoundFacet(prop, value));

    if (draft != null) {
      String key = prop.getPropertyName();
      if (draft.get(key, value.getClass()) != v) {
        draft.set(key, v);
      }
    }

    if (pojo != null) {
      if (!BeanColumnValueProvider.INSTANCE.getColumnValue(pojo, -1, prop).equals(v)) {
        String key = prop.getPropertyName();
        ((MapExportable) pojo).put(key, v);
      }
    }

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

    BoundFacet facet = null;
    HelenusProperty prop = p.getProperty();
    if (pojo != null) {
      Long value = (Long) BeanColumnValueProvider.INSTANCE.getColumnValue(pojo, -1, prop);
      facet = new BoundFacet(prop, value + delta);
    } else if (draft != null) {
      String key = prop.getPropertyName();
      draftMap.put(key, (Long) draftMap.get(key) + delta);
      facet = new BoundFacet(prop, draftMap.get(key));
    }

    assignments.put(QueryBuilder.incr(p.getColumnName(), delta), facet);

    addPropertyNode(p);

    return this;
  }

  public <V> UpdateOperation<E> decrement(Getter<V> counterGetter) {
    return decrement(counterGetter, 1L);
  }

  public <V> UpdateOperation<E> decrement(Getter<V> counterGetter, long delta) {

    Objects.requireNonNull(counterGetter, "counterGetter is empty");

    HelenusPropertyNode p = MappingUtil.resolveMappingProperty(counterGetter);

    BoundFacet facet = null;
    HelenusProperty prop = p.getProperty();
    if (pojo != null) {
      Long value = (Long) BeanColumnValueProvider.INSTANCE.getColumnValue(pojo, -1, prop);
      facet = new BoundFacet(prop, value - delta);
    } else if (draft != null) {
      String key = prop.getPropertyName();
      draftMap.put(key, (Long) draftMap.get(key) - delta);
      facet = new BoundFacet(prop, draftMap.get(key));
    }

    assignments.put(QueryBuilder.decr(p.getColumnName(), delta), facet);

    addPropertyNode(p);

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

    final List<V> list;
    final BoundFacet facet;
    HelenusProperty prop = p.getProperty();
    if (pojo != null) {
      list = (List<V>) BeanColumnValueProvider.INSTANCE.getColumnValue(pojo, -1, prop, false);
      list.add(0, value);
      facet = new BoundFacet(prop, list);
    } else if (draft != null) {
      String key = prop.getPropertyName();
      list = (List<V>) draftMap.get(key);
      list.add(0, value);
      facet = new BoundFacet(prop, list);
    } else {
      list = null;
      facet = null;
    }

    assignments.put(QueryBuilder.prepend(p.getColumnName(), valueObj), facet);

    addPropertyNode(p);

    return this;
  }

  public <V> UpdateOperation<E> prependAll(Getter<List<V>> listGetter, List<V> value) {

    Objects.requireNonNull(listGetter, "listGetter is empty");
    Objects.requireNonNull(value, "value is empty");

    HelenusPropertyNode p = MappingUtil.resolveMappingProperty(listGetter);
    List valueObj = prepareListValue(p, value);

    final List<V> list;
    final BoundFacet facet;
    HelenusProperty prop = p.getProperty();
    if (pojo != null) {
      list = (List<V>) BeanColumnValueProvider.INSTANCE.getColumnValue(pojo, -1, prop, false);
      list.addAll(0, value);
      facet = new BoundFacet(prop, list);
    } else if (draft != null && value.size() > 0) {
      String key = p.getProperty().getPropertyName();
      list = (List<V>) draftMap.get(key);
      list.addAll(0, value);
      facet = new BoundFacet(prop, list);
    } else {
      list = null;
      facet = null;
    }

    assignments.put(QueryBuilder.prependAll(p.getColumnName(), valueObj), facet);

    addPropertyNode(p);

    return this;
  }

  public <V> UpdateOperation<E> setIdx(Getter<List<V>> listGetter, int idx, V value) {

    Objects.requireNonNull(listGetter, "listGetter is empty");
    Objects.requireNonNull(value, "value is empty");

    HelenusPropertyNode p = MappingUtil.resolveMappingProperty(listGetter);
    Object valueObj = prepareSingleListValue(p, value);

    final BoundFacet facet;
    HelenusProperty prop = p.getProperty();
    if (pojo != null || draft != null) {
      final List<V> list;
      if (pojo != null) {
        list = (List<V>) BeanColumnValueProvider.INSTANCE.getColumnValue(pojo, -1, prop, false);
      } else {
        String key = prop.getPropertyName();
        list = (List<V>) draftMap.get(key);
      }
      if (idx < 0) {
        list.add(0, value);
      } else if (idx > list.size()) {
        list.add(list.size(), value);
      } else {
        list.add(idx, value);
      }
      list.add(0, value);
      facet = new BoundFacet(prop, list);
    } else {
      facet = null;
    }

    assignments.put(QueryBuilder.setIdx(p.getColumnName(), idx, valueObj), facet);

    addPropertyNode(p);

    return this;
  }

  public <V> UpdateOperation<E> append(Getter<List<V>> listGetter, V value) {

    Objects.requireNonNull(listGetter, "listGetter is empty");
    Objects.requireNonNull(value, "value is empty");

    HelenusPropertyNode p = MappingUtil.resolveMappingProperty(listGetter);
    Object valueObj = prepareSingleListValue(p, value);

    final List<V> list;
    final BoundFacet facet;
    HelenusProperty prop = p.getProperty();
    if (pojo != null) {
      list = (List<V>) BeanColumnValueProvider.INSTANCE.getColumnValue(pojo, -1, prop, false);
      list.add(value);
      facet = new BoundFacet(prop, list);
    } else if (draft != null) {
      String key = prop.getPropertyName();
      list = (List<V>) draftMap.get(key);
      list.add(value);
      facet = new BoundFacet(prop, list);
    } else {
      list = null;
      facet = null;
    }
    assignments.put(QueryBuilder.append(p.getColumnName(), valueObj), facet);

    addPropertyNode(p);

    return this;
  }

  public <V> UpdateOperation<E> appendAll(Getter<List<V>> listGetter, List<V> value) {

    Objects.requireNonNull(listGetter, "listGetter is empty");
    Objects.requireNonNull(value, "value is empty");

    HelenusPropertyNode p = MappingUtil.resolveMappingProperty(listGetter);
    List valueObj = prepareListValue(p, value);

    final List<V> list;
    final BoundFacet facet;
    HelenusProperty prop = p.getProperty();
    if (pojo != null) {
      list = (List<V>) BeanColumnValueProvider.INSTANCE.getColumnValue(pojo, -1, prop, false);
      list.addAll(value);
      facet = new BoundFacet(prop, list);
    } else if (draft != null && value.size() > 0) {
      String key = prop.getPropertyName();
      list = (List<V>) draftMap.get(key);
      list.addAll(value);
      facet = new BoundFacet(prop, list);
    } else {
      list = null;
      facet = null;
    }
    assignments.put(QueryBuilder.appendAll(p.getColumnName(), valueObj), facet);

    addPropertyNode(p);

    return this;
  }

  public <V> UpdateOperation<E> discard(Getter<List<V>> listGetter, V value) {

    Objects.requireNonNull(listGetter, "listGetter is empty");
    Objects.requireNonNull(value, "value is empty");

    HelenusPropertyNode p = MappingUtil.resolveMappingProperty(listGetter);
    Object valueObj = prepareSingleListValue(p, value);

    final List<V> list;
    final BoundFacet facet;
    HelenusProperty prop = p.getProperty();
    if (pojo != null) {
      list = (List<V>) BeanColumnValueProvider.INSTANCE.getColumnValue(pojo, -1, prop, false);
      list.remove(value);
      facet = new BoundFacet(prop, list);
    } else if (draft != null) {
      String key = prop.getPropertyName();
      list = (List<V>) draftMap.get(key);
      list.remove(value);
      facet = new BoundFacet(prop, list);
    } else {
      list = null;
      facet = null;
    }
    assignments.put(QueryBuilder.discard(p.getColumnName(), valueObj), facet);

    addPropertyNode(p);

    return this;
  }

  public <V> UpdateOperation<E> discardAll(Getter<List<V>> listGetter, List<V> value) {

    Objects.requireNonNull(listGetter, "listGetter is empty");
    Objects.requireNonNull(value, "value is empty");

    HelenusPropertyNode p = MappingUtil.resolveMappingProperty(listGetter);
    List valueObj = prepareListValue(p, value);

    final List<V> list;
    final BoundFacet facet;
    HelenusProperty prop = p.getProperty();
    if (pojo != null) {
      list = (List<V>) BeanColumnValueProvider.INSTANCE.getColumnValue(pojo, -1, prop, false);
      list.removeAll(value);
      facet = new BoundFacet(prop, list);
    } else if (draft != null) {
      String key = prop.getPropertyName();
      list = (List<V>) draftMap.get(key);
      list.removeAll(value);
      facet = new BoundFacet(prop, list);
    } else {
      list = null;
      facet = null;
    }
    assignments.put(QueryBuilder.discardAll(p.getColumnName(), valueObj), facet);

    addPropertyNode(p);

    return this;
  }

  private Object prepareSingleListValue(HelenusPropertyNode p, Object value) {
    HelenusProperty prop = p.getProperty();

    Object valueObj = value;

    Optional<Function<Object, Object>> converter =
        prop.getWriteConverter(sessionOps.getSessionRepository());
    if (converter.isPresent()) {
      List convertedList = (List) converter.get().apply(Immutables.listOf(value));
      valueObj = convertedList.get(0);
    }

    return valueObj;
  }

  private List prepareListValue(HelenusPropertyNode p, List value) {

    HelenusProperty prop = p.getProperty();

    List valueObj = value;

    Optional<Function<Object, Object>> converter =
        prop.getWriteConverter(sessionOps.getSessionRepository());
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

    final Set<V> set;
    final BoundFacet facet;
    HelenusProperty prop = p.getProperty();
    if (pojo != null) {
      set = (Set<V>) BeanColumnValueProvider.INSTANCE.getColumnValue(pojo, -1, prop, false);
      set.add(value);
      facet = new BoundFacet(prop, set);
    } else if (draft != null) {
      String key = prop.getPropertyName();
      set = (Set<V>) draftMap.get(key);
      set.add(value);
      facet = new BoundFacet(prop, set);
    } else {
      set = null;
      facet = null;
    }
    assignments.put(QueryBuilder.add(p.getColumnName(), valueObj), facet);

    addPropertyNode(p);

    return this;
  }

  public <V> UpdateOperation<E> addAll(Getter<Set<V>> setGetter, Set<V> value) {

    Objects.requireNonNull(setGetter, "setGetter is empty");
    Objects.requireNonNull(value, "value is empty");

    HelenusPropertyNode p = MappingUtil.resolveMappingProperty(setGetter);
    Set valueObj = prepareSetValue(p, value);

    final Set<V> set;
    final BoundFacet facet;
    HelenusProperty prop = p.getProperty();
    if (pojo != null) {
      set = (Set<V>) BeanColumnValueProvider.INSTANCE.getColumnValue(pojo, -1, prop, false);
      set.addAll(value);
      facet = new BoundFacet(prop, set);
    } else if (draft != null) {
      String key = prop.getPropertyName();
      set = (Set<V>) draftMap.get(key);
      set.addAll(value);
      facet = new BoundFacet(prop, set);
    } else {
      set = null;
      facet = null;
    }
    assignments.put(QueryBuilder.addAll(p.getColumnName(), valueObj), facet);

    addPropertyNode(p);

    return this;
  }

  public <V> UpdateOperation<E> remove(Getter<Set<V>> setGetter, V value) {

    Objects.requireNonNull(setGetter, "setGetter is empty");
    Objects.requireNonNull(value, "value is empty");

    HelenusPropertyNode p = MappingUtil.resolveMappingProperty(setGetter);
    Object valueObj = prepareSingleSetValue(p, value);

    final Set<V> set;
    final BoundFacet facet;
    HelenusProperty prop = p.getProperty();
    if (pojo != null) {
      set = (Set<V>) BeanColumnValueProvider.INSTANCE.getColumnValue(pojo, -1, prop, false);
      set.remove(value);
      facet = new BoundFacet(prop, set);
    } else if (draft != null) {
      String key = prop.getPropertyName();
      set = (Set<V>) draftMap.get(key);
      set.remove(value);
      facet = new BoundFacet(prop, set);
    } else {
      set = null;
      facet = null;
    }
    assignments.put(QueryBuilder.remove(p.getColumnName(), valueObj), facet);

    addPropertyNode(p);

    return this;
  }

  public <V> UpdateOperation<E> removeAll(Getter<Set<V>> setGetter, Set<V> value) {

    Objects.requireNonNull(setGetter, "setGetter is empty");
    Objects.requireNonNull(value, "value is empty");

    HelenusPropertyNode p = MappingUtil.resolveMappingProperty(setGetter);
    Set valueObj = prepareSetValue(p, value);

    final Set<V> set;
    final BoundFacet facet;
    HelenusProperty prop = p.getProperty();
    if (pojo != null) {
      set = (Set<V>) BeanColumnValueProvider.INSTANCE.getColumnValue(pojo, -1, prop, false);
      set.removeAll(value);
      facet = new BoundFacet(prop, set);
    } else if (draft != null) {
      String key = prop.getPropertyName();
      set = (Set<V>) draftMap.get(key);
      set.removeAll(value);
      facet = new BoundFacet(prop, set);
    } else {
      set = null;
      facet = null;
    }
    assignments.put(QueryBuilder.removeAll(p.getColumnName(), valueObj), facet);

    addPropertyNode(p);

    return this;
  }

  private Object prepareSingleSetValue(HelenusPropertyNode p, Object value) {

    HelenusProperty prop = p.getProperty();
    Object valueObj = value;

    Optional<Function<Object, Object>> converter =
        prop.getWriteConverter(sessionOps.getSessionRepository());
    if (converter.isPresent()) {
      Set convertedSet = (Set) converter.get().apply(Immutables.setOf(value));
      valueObj = convertedSet.iterator().next();
    }

    return valueObj;
  }

  private Set prepareSetValue(HelenusPropertyNode p, Set value) {

    HelenusProperty prop = p.getProperty();
    Set valueObj = value;

    Optional<Function<Object, Object>> converter =
        prop.getWriteConverter(sessionOps.getSessionRepository());
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

    final Map<K, V> map;
    final BoundFacet facet;
    if (pojo != null) {
      map = (Map<K, V>) BeanColumnValueProvider.INSTANCE.getColumnValue(pojo, -1, prop, false);
      map.put(key, value);
      facet = new BoundFacet(prop, map);
    } else if (draft != null) {
      map = (Map<K, V>) draftMap.get(prop.getPropertyName());
      map.put(key, value);
      facet = new BoundFacet(prop, map);
    } else {
      map = null;
      facet = null;
    }

    Optional<Function<Object, Object>> converter =
        prop.getWriteConverter(sessionOps.getSessionRepository());
    if (converter.isPresent()) {
      Map<Object, Object> convertedMap =
          (Map<Object, Object>) converter.get().apply(Immutables.mapOf(key, value));
      for (Map.Entry<Object, Object> e : convertedMap.entrySet()) {
        assignments.put(QueryBuilder.put(p.getColumnName(), e.getKey(), e.getValue()), facet);
      }
    } else {
      assignments.put(QueryBuilder.put(p.getColumnName(), key, value), facet);
    }

    addPropertyNode(p);

    return this;
  }

  public <K, V> UpdateOperation<E> putAll(Getter<Map<K, V>> mapGetter, Map<K, V> map) {

    Objects.requireNonNull(mapGetter, "mapGetter is empty");
    Objects.requireNonNull(map, "map is empty");

    HelenusPropertyNode p = MappingUtil.resolveMappingProperty(mapGetter);
    HelenusProperty prop = p.getProperty();

    final Map<K, V> newMap;
    final BoundFacet facet;
    if (pojo != null) {
      newMap = (Map<K, V>) BeanColumnValueProvider.INSTANCE.getColumnValue(pojo, -1, prop, false);
      newMap.putAll(map);
      facet = new BoundFacet(prop, newMap);
    } else if (draft != null) {
      newMap = (Map<K, V>) draftMap.get(prop.getPropertyName());
      newMap.putAll(map);
      facet = new BoundFacet(prop, newMap);
    } else {
      newMap = null;
      facet = null;
    }

    Optional<Function<Object, Object>> converter =
        prop.getWriteConverter(sessionOps.getSessionRepository());
    if (converter.isPresent()) {
      Map convertedMap = (Map) converter.get().apply(map);
      assignments.put(QueryBuilder.putAll(p.getColumnName(), convertedMap), facet);
    } else {
      assignments.put(QueryBuilder.putAll(p.getColumnName(), map), facet);
    }

    addPropertyNode(p);

    return this;
  }

  @Override
  public BuiltStatement buildStatement(boolean cached) {

    if (entity == null) {
      throw new HelenusMappingException("empty update operation");
    }

    Update update = QueryBuilder.update(entity.getName().toCql());

    for (Assignment assignment : assignments.keySet()) {
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
    if ((ifFilters != null && !ifFilters.isEmpty()) && (resultSet.wasApplied() == false)) {
      throw new HelenusException("Statement was not applied due to consistency constraints");
    }

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
      throw new HelenusMappingException(
          "you can update columns only in single entity "
              + entity.getMappingInterface()
              + " or "
              + p.getEntity().getMappingInterface());
    }
  }

  private void adjustTtlAndWriteTime(MapExportable pojo) {
    if (ttl != null || writeTime != 0L) {
      List<String> names = new ArrayList<String>(assignments.size());
      for (BoundFacet facet : assignments.values()) {
        for (HelenusProperty prop : facet.getProperties()) {
          names.add(prop.getColumnName().toCql(false));
        }
      }

      if (names.size() > 0) {
        if (ttl != null) {
          names.forEach(name -> pojo.put(CacheUtil.ttlKey(name), ttl));
        }
        if (writeTime != 0L) {
          names.forEach(name -> pojo.put(CacheUtil.writeTimeKey(name), writeTime));
        }
      }
    }
  }

  @Override
  public E sync() throws TimeoutException {
    E result = super.sync();
    if (result != null && entity.isCacheable()) {
      if (draft != null) {
        adjustTtlAndWriteTime(draft);
        adjustTtlAndWriteTime((MapExportable) result);
        sessionOps.updateCache(result, bindFacetValues());
      } else if (pojo != null) {
        adjustTtlAndWriteTime((MapExportable) pojo);
        sessionOps.updateCache(pojo, bindFacetValues());
      } else {
        sessionOps.cacheEvict(bindFacetValues());
      }
    }
    return result;
  }

  @Override
  public E sync(UnitOfWork uow) throws TimeoutException {
    if (uow == null) {
      return sync();
    }
    E result = super.sync(uow);
    if (result != null) {
      if (draft != null) {
        adjustTtlAndWriteTime(draft);
      }
      if (entity != null && MapExportable.class.isAssignableFrom(entity.getMappingInterface())) {
        adjustTtlAndWriteTime((MapExportable) result);
        cacheUpdate(uow, result, bindFacetValues());
      } else if (pojo != null) {
        adjustTtlAndWriteTime((MapExportable) pojo);
        cacheUpdate(uow, (E) pojo, bindFacetValues());
        return (E) pojo;
      }
    }
    return result;
  }

  public E batch(UnitOfWork uow) throws TimeoutException {
    if (uow == null) {
      throw new HelenusException("UnitOfWork cannot be null when batching operations.");
    }

    final E result;
    if (draft != null) {
      result = draft.build();
      adjustTtlAndWriteTime(draft);
    } else if (pojo != null) {
      result = (E) pojo;
      adjustTtlAndWriteTime((MapExportable) pojo);
    } else {
      result = null;
    }

    if (result != null) {
      cacheUpdate(uow, result, bindFacetValues());
      uow.batch(this);
      return result;
    }

    return sync(uow);
  }

  @Override
  public List<Facet> bindFacetValues() {
    List<Facet> facets = bindFacetValues(entity.getFacets());
    facets.addAll(
        assignments
            .values()
            .stream()
            .distinct()
            .filter(o -> o != null)
            .collect(Collectors.toList()));
    return facets;
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
