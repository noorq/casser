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
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import net.helenus.core.AbstractSessionOperations;
import net.helenus.core.Getter;
import net.helenus.core.Helenus;
import net.helenus.core.UnitOfWork;
import net.helenus.core.cache.CacheUtil;
import net.helenus.core.cache.Facet;
import net.helenus.core.cache.UnboundFacet;
import net.helenus.core.reflect.DefaultPrimitiveTypes;
import net.helenus.core.reflect.Drafted;
import net.helenus.core.reflect.HelenusPropertyNode;
import net.helenus.core.reflect.MapExportable;
import net.helenus.mapping.HelenusEntity;
import net.helenus.mapping.HelenusProperty;
import net.helenus.mapping.MappingUtil;
import net.helenus.mapping.value.BeanColumnValueProvider;
import net.helenus.support.Fun;
import net.helenus.support.HelenusException;
import net.helenus.support.HelenusMappingException;

import static net.helenus.mapping.ColumnType.CLUSTERING_COLUMN;
import static net.helenus.mapping.ColumnType.PARTITION_KEY;

public final class InsertOperation<T> extends AbstractOperation<T, InsertOperation<T>> {

  private final List<Fun.Tuple2<HelenusPropertyNode, Object>> values = new ArrayList<Fun.Tuple2<HelenusPropertyNode, Object>>();
  private final T pojo;
  private final Class<?> resultType;
  private HelenusEntity entity;
  private boolean ifNotExists;

  private int[] ttl;
  private long[] timestamp;

  public InsertOperation(AbstractSessionOperations sessionOperations, boolean ifNotExists) {
    super(sessionOperations);

    this.ifNotExists = ifNotExists;
    this.pojo = null;
    this.resultType = ResultSet.class;
  }

  public InsertOperation(AbstractSessionOperations sessionOperations, Class<?> resultType, boolean ifNotExists) {
    super(sessionOperations);

    this.ifNotExists = ifNotExists;
    this.pojo = null;
    this.resultType = resultType;
  }

  public InsertOperation(AbstractSessionOperations sessionOperations, HelenusEntity entity, T pojo,
                         Set<String> mutations, boolean ifNotExists) {
    super(sessionOperations);

    this.entity = entity;
    this.pojo = pojo;
    this.ifNotExists = ifNotExists;
    this.resultType = entity.getMappingInterface();

    Collection<HelenusProperty> properties = entity.getOrderedProperties();
    Set<String> keys = (mutations == null) ? null : mutations;

    for (HelenusProperty prop : properties) {
      boolean addProp = false;

      switch (prop.getColumnType()) {
        case PARTITION_KEY:
        case CLUSTERING_COLUMN:
          addProp = true;
          break;
        default:
          addProp = (keys == null || keys.contains(prop.getPropertyName()));
      }

      if (addProp) {
        Object value = BeanColumnValueProvider.INSTANCE.getColumnValue(pojo, -1, prop);
        value = sessionOps.getValuePreparer().prepareColumnValue(value, prop);

        if (value != null) {
          HelenusPropertyNode node = new HelenusPropertyNode(prop, Optional.empty());
          values.add(Fun.Tuple2.of(node, value));
        }
      }
    }
  }

  public InsertOperation<T> ifNotExists() {
    this.ifNotExists = true;
    return this;
  }

  public InsertOperation<T> ifNotExists(boolean enable) {
    this.ifNotExists = enable;
    return this;
  }

  public <V> InsertOperation<T> value(Getter<V> getter, V val) {

    Objects.requireNonNull(getter, "getter is empty");

    if (val != null) {
      HelenusPropertyNode node = MappingUtil.resolveMappingProperty(getter);
      Object value = sessionOps.getValuePreparer().prepareColumnValue(val, node.getProperty());

      if (value != null) {
        values.add(Fun.Tuple2.of(node, value));
      }
    }

    return this;
  }

  @Override
  public BuiltStatement buildStatement(boolean cached) {

    values.forEach(t -> addPropertyNode(t._1));

    if (values.isEmpty()) return null;

    if (entity == null) {
      throw new HelenusMappingException("unknown entity");
    }

    Insert insert = QueryBuilder.insertInto(entity.getName().toCql());

    if (ifNotExists) {
      insert.ifNotExists();
    }

    values.forEach(
        t -> {
          insert.value(t._1.getColumnName(), t._2);
        });

    //TODO(gburd): IF NOT EXISTS when @Constraints.Relationship is 1:1 or 1:m

    if (this.ttl != null) {
      insert.using(QueryBuilder.ttl(this.ttl[0]));
    }
    if (this.timestamp != null) {
      insert.using(QueryBuilder.timestamp(this.timestamp[0]));
    }

    return insert;
  }

  @Override
  public T transform(ResultSet resultSet) {
    if ((ifNotExists == true) && (resultSet.wasApplied() == false)) {
      throw new HelenusException("Statement was not applied due to consistency constraints");
    }

    Class<?> iface = entity.getMappingInterface();
    if (resultType == iface) {
      if (values.size() > 0) {
        boolean immutable = iface.isAssignableFrom(Drafted.class);
        Collection<HelenusProperty> properties = entity.getOrderedProperties();
        Map<String, Object> backingMap = new HashMap<String, Object>(properties.size());

        // First, add all the inserted values into our new map.
        values.forEach(t -> backingMap.put(t._1.getProperty().getPropertyName(), t._2));

        // Then, fill in all the rest of the properties.
        for (HelenusProperty prop : properties) {
          String key = prop.getPropertyName();
          if (backingMap.containsKey(key)) {
            // Some values man need to be converted (e.g. from String to Enum). This is done
            // within the BeanColumnValueProvider below.
            Optional<Function<Object, Object>> converter =
                prop.getReadConverter(sessionOps.getSessionRepository());
            if (converter.isPresent()) {
              backingMap.put(key, converter.get().apply(backingMap.get(key)));
            }
          } else {
            // If we started this operation with an instance of this type, use values from
            // that.
            if (pojo != null) {
              backingMap.put(
                  key, BeanColumnValueProvider.INSTANCE.getColumnValue(pojo, -1, prop, immutable));
            } else {
              // Otherwise we'll use default values for the property type if available.
              Class<?> propType = prop.getJavaType();
              if (propType.isPrimitive()) {
                DefaultPrimitiveTypes type = DefaultPrimitiveTypes.lookup(propType);
                if (type == null) {
                  throw new HelenusException("unknown primitive type " + propType);
                }
                backingMap.put(key, type.getDefaultValue());
              }
            }
          }
        }

        // Lastly, create a new proxy object for the entity and return the new instance.
        return (T) Helenus.map(iface, backingMap);
      }
      // Oddly, this insert didn't change anything so simply return the pojo.
      return (T) pojo;
    }
    return (T) resultSet;
  }

  public InsertOperation<T> usingTtl(int ttl) {
    this.ttl = new int[1];
    this.ttl[0] = ttl;
    return this;
  }

  public InsertOperation<T> usingTimestamp(long timestamp) {
    this.timestamp = new long[1];
    this.timestamp[0] = timestamp;
    return this;
  }

  private void addPropertyNode(HelenusPropertyNode p) {
    if (entity == null) {
      entity = p.getEntity();
    } else if (entity != p.getEntity()) {
      throw new HelenusMappingException(
          "you can insert only single entity "
              + entity.getMappingInterface()
              + " or "
              + p.getEntity().getMappingInterface());
    }
  }

  protected void adjustTtlAndWriteTime(MapExportable pojo) {
    if (ttl != null || timestamp != null) {
      List<String> propertyNames = values.stream()
              .map(t -> t._1.getProperty())
              .filter(prop -> {
                switch (prop.getColumnType()) {
                  case PARTITION_KEY:
                  case CLUSTERING_COLUMN:
                    return false;
                  default:
                    return true;
                }
              })
              .map(prop -> prop.getColumnName().toCql(true))
              .collect(Collectors.toList());

      if (propertyNames.size() > 0) {
        if (ttl != null) {
          propertyNames.forEach(name -> pojo.put(CacheUtil.ttlKey(name), ttl));
        }
        if (timestamp != null) {
          propertyNames.forEach(name -> pojo.put(CacheUtil.writeTimeKey(name), timestamp));
        }
      }
    }
  }

  @Override
  public T sync() throws TimeoutException {
    T result = super.sync();
    if (entity.isCacheable() && result != null) {
      sessionOps.updateCache(result, bindFacetValues());
      adjustTtlAndWriteTime((MapExportable)result);
    }
    return result;
  }

  @Override
  public T sync(UnitOfWork uow) throws TimeoutException {
    if (uow == null) {
      return sync();
    }
    T result = super.sync(uow);
    if (result != null && pojo != null && !(pojo == result) && pojo.equals(result)) {
      // To preserve object identity we need to find this object in cache
      // because it was unchanged by the INSERT but pojo in this case was
      // the result of a draft.build().
      T cachedValue = (T) uow.cacheLookup(bindFacetValues());
      if (cachedValue != null) {
        result = cachedValue;
      }
    }
    Class<?> iface = entity.getMappingInterface();
    if (resultType == iface) {
      cacheUpdate(uow, result, bindFacetValues());
      adjustTtlAndWriteTime((MapExportable)pojo);
    } else {
      if (entity.isCacheable()) {
        sessionOps.cacheEvict(bindFacetValues());
      }
    }
    return result;
  }

  public T batch(UnitOfWork uow) throws TimeoutException {
    if (uow == null) {
      throw new HelenusException("UnitOfWork cannot be null when batching operations.");
    }

    if (this.entity != null && pojo != null) {
      Class<?> iface = this.entity.getMappingInterface();
      if (resultType == iface) {
        cacheUpdate(uow, pojo, bindFacetValues());
        adjustTtlAndWriteTime((MapExportable)pojo);
        uow.batch(this);
        return (T) pojo;
      }
    }

    return sync(uow);
  }

  @Override
  public List<Facet> bindFacetValues() {
    List<Facet> facets = getFacets();
    if (facets == null || facets.size() == 0) {
      return new ArrayList<Facet>();
    }
    List<Facet> boundFacets = new ArrayList<>();
    Map<HelenusProperty, Object> valuesMap = new HashMap<>(values.size());
    values.forEach(t -> valuesMap.put(t._1.getProperty(), t._2));

    for (Facet facet : facets) {
      if (facet instanceof UnboundFacet) {
        UnboundFacet unboundFacet = (UnboundFacet) facet;
        UnboundFacet.Binder binder = unboundFacet.binder();
        for (HelenusProperty prop : unboundFacet.getProperties()) {
          Object value = valuesMap.get(prop);
          if (value != null) {
            binder.setValueForProperty(prop, value.toString());
          }
        }
        if (binder.isBound()) {
          boundFacets.add(binder.bind());
        }
      } else {
        boundFacets.add(facet);
      }
    }
    return boundFacets;
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
