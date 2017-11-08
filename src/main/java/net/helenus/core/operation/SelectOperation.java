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
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Ordering;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.google.common.collect.Iterables;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import net.helenus.core.*;
import net.helenus.core.cache.CacheUtil;
import net.helenus.core.cache.Facet;
import net.helenus.core.cache.UnboundFacet;
import net.helenus.core.reflect.Entity;
import net.helenus.core.reflect.HelenusPropertyNode;
import net.helenus.mapping.HelenusEntity;
import net.helenus.mapping.HelenusProperty;
import net.helenus.mapping.MappingUtil;
import net.helenus.mapping.OrderingDirection;
import net.helenus.mapping.value.ColumnValueProvider;
import net.helenus.mapping.value.ValueProviderMap;
import net.helenus.support.Fun;
import net.helenus.support.HelenusMappingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SelectOperation<E> extends AbstractFilterStreamOperation<E, SelectOperation<E>> {

  private static final Logger LOG = LoggerFactory.getLogger(SelectOperation.class);

  protected final List<HelenusPropertyNode> props = new ArrayList<HelenusPropertyNode>();
  protected Function<Row, E> rowMapper = null;
  protected List<Ordering> ordering = null;
  protected Integer limit = null;
  protected boolean allowFiltering = false;

  protected String alternateTableName = null;
  protected boolean isCacheable = false;
  protected boolean implementsEntityType = false;

  @SuppressWarnings("unchecked")
  public SelectOperation(AbstractSessionOperations sessionOperations) {
    super(sessionOperations);

    this.rowMapper =
        new Function<Row, E>() {

          @Override
          public E apply(Row source) {

            ColumnValueProvider valueProvider = sessionOps.getValueProvider();
            Object[] arr = new Object[props.size()];

            int i = 0;
            for (HelenusPropertyNode p : props) {
              Object value = valueProvider.getColumnValue(source, -1, p.getProperty());
              arr[i++] = value;
            }

            return (E) Fun.ArrayTuple.of(arr);
          }
        };
  }

  public SelectOperation(AbstractSessionOperations sessionOperations, HelenusEntity entity) {

    super(sessionOperations);

    entity
        .getOrderedProperties()
        .stream()
        .map(p -> new HelenusPropertyNode(p, Optional.empty()))
        .forEach(p -> this.props.add(p));

    this.isCacheable = entity.isCacheable();
    this.implementsEntityType = MappingUtil.extendsInterface(entity.getMappingInterface(), Entity.class);
  }

  public SelectOperation(
      AbstractSessionOperations sessionOperations,
      HelenusEntity entity,
      Function<Row, E> rowMapper) {

    super(sessionOperations);
    this.rowMapper = rowMapper;

    entity
        .getOrderedProperties()
        .stream()
        .map(p -> new HelenusPropertyNode(p, Optional.empty()))
        .forEach(p -> this.props.add(p));

    this.isCacheable = entity.isCacheable();
    this.implementsEntityType = MappingUtil.extendsInterface(entity.getMappingInterface(), Entity.class);
  }

  public SelectOperation(AbstractSessionOperations sessionOperations, Function<Row, E> rowMapper,
                         HelenusPropertyNode... props) {

    super(sessionOperations);

    this.rowMapper = rowMapper;
    Collections.addAll(this.props, props);

    HelenusEntity entity = props[0].getEntity();
    this.isCacheable = entity.isCacheable();
    this.implementsEntityType = MappingUtil.extendsInterface(entity.getMappingInterface(), Entity.class);
  }

  public CountOperation count() {

    HelenusEntity entity = null;
    for (HelenusPropertyNode prop : props) {

      if (entity == null) {
        entity = prop.getEntity();
      } else if (entity != prop.getEntity()) {
        throw new HelenusMappingException(
            "you can count records only from a single entity "
                + entity.getMappingInterface()
                + " or "
                + prop.getEntity().getMappingInterface());
      }
    }

    return new CountOperation(sessionOps, entity);
  }

  public <V extends E> SelectOperation<E> from(Class<V> materializedViewClass) {
    Objects.requireNonNull(materializedViewClass);
    HelenusEntity entity = Helenus.entity(materializedViewClass);
    this.alternateTableName = entity.getName().toCql();
    this.props.clear();
    entity
        .getOrderedProperties()
        .stream()
        .map(p -> new HelenusPropertyNode(p, Optional.empty()))
        .forEach(p -> this.props.add(p));
    return this;
  }

  public SelectFirstOperation<E> single() {
    limit(1);
    return new SelectFirstOperation<E>(this);
  }

  public <R> SelectTransformingOperation<R, E> mapTo(Class<R> entityClass) {

    Objects.requireNonNull(entityClass, "entityClass is null");

    HelenusEntity entity = Helenus.entity(entityClass);

    this.rowMapper = null;

    return new SelectTransformingOperation<R, E>(
        this,
        (r) -> {
          Map<String, Object> map = new ValueProviderMap(r, sessionOps.getValueProvider(), entity);
          return (R) Helenus.map(entityClass, map);
        });
  }

  public <R> SelectTransformingOperation<R, E> map(Function<E, R> fn) {
    return new SelectTransformingOperation<R, E>(this, fn);
  }

  public SelectOperation<E> column(Getter<?> getter) {
    HelenusPropertyNode p = MappingUtil.resolveMappingProperty(getter);
    this.props.add(p);
    return this;
  }

  public SelectOperation<E> orderBy(Getter<?> getter, OrderingDirection direction) {
    getOrCreateOrdering().add(new Ordered(getter, direction).getOrdering());
    return this;
  }

  public SelectOperation<E> orderBy(Ordered ordered) {
    getOrCreateOrdering().add(ordered.getOrdering());
    return this;
  }

  public SelectOperation<E> limit(Integer limit) {
    this.limit = limit;
    return this;
  }

  public SelectOperation<E> allowFiltering() {
    this.allowFiltering = true;
    return this;
  }

  @Override
  public boolean isSessionCacheable() {
    return isCacheable;
  }

  @Override
  public List<Facet> getFacets() {
    HelenusEntity entity = props.get(0).getEntity();
    return entity.getFacets();
  }

  @Override
  public List<Facet> bindFacetValues() {
    HelenusEntity entity = props.get(0).getEntity();
    List<Facet> boundFacets = new ArrayList<>();

    for (Facet facet : entity.getFacets()) {
      if (facet instanceof UnboundFacet) {
        UnboundFacet unboundFacet = (UnboundFacet) facet;
        UnboundFacet.Binder binder = unboundFacet.binder();
        for (HelenusProperty prop : unboundFacet.getProperties()) {
          if (filters != null) {
            Filter filter = filters.get(prop);
            if (filter != null) {
              Object[] postulates = filter.postulateValues();
              for (Object p : postulates) {
                binder.setValueForProperty(prop, p.toString());
              }
            }
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
  public BuiltStatement buildStatement(boolean cached) {

    HelenusEntity entity = null;
    Selection selection = QueryBuilder.select();

    for (HelenusPropertyNode prop : props) {
      String columnName = prop.getColumnName();
      selection = selection.column(columnName);

      if (entity == null) {
        entity = prop.getEntity();
      } else if (entity != prop.getEntity()) {
        throw new HelenusMappingException(
            "you can select columns only from a single entity "
                + entity.getMappingInterface()
                + " or "
                + prop.getEntity().getMappingInterface());
      }

      if (cached && implementsEntityType) {
        switch (prop.getProperty().getColumnType()) {
          case PARTITION_KEY:
          case CLUSTERING_COLUMN:
            break;
          default:
            if (entity.equals(prop.getEntity())) {
              if (!prop.getProperty().getDataType().isCollectionType()) {
                columnName = prop.getProperty().getColumnName().toCql(false);
                selection.ttl(columnName).as('"' + CacheUtil.ttlKey(columnName) + '"');
                selection.writeTime(columnName).as('"' + CacheUtil.writeTimeKey(columnName) + '"');
              }
            }
            break;
        }
      }
    }

    if (entity == null) {
      throw new HelenusMappingException("no entity or table to select data");
    }

    String tableName = alternateTableName == null ? entity.getName().toCql() : alternateTableName;
    Select select = selection.from(tableName);

    if (ordering != null && !ordering.isEmpty()) {
      select.orderBy(ordering.toArray(new Ordering[ordering.size()]));
    }

    if (limit != null) {
      select.limit(limit);
    }

    if (filters != null && !filters.isEmpty()) {

      Where where = select.where();

      boolean isFirstIndex = true;
      for (Filter<?> filter : filters.values()) {
        where.and(filter.getClause(sessionOps.getValuePreparer()));
        HelenusProperty filterProp = filter.getNode().getProperty();
        HelenusProperty prop = props.stream()
                .map(HelenusPropertyNode::getProperty)
                .filter(thisProp -> thisProp.getPropertyName().equals(filterProp.getPropertyName()))
                .findFirst()
                .orElse(null);
        if (allowFiltering == false && prop != null) {
          switch (prop.getColumnType()) {
            case PARTITION_KEY:
              break;
            case CLUSTERING_COLUMN:
            default:
              // When using non-Cassandra-standard 2i types or when using more than one
              // indexed column or non-indexed columns the query must include ALLOW FILTERING.
              if (prop.caseSensitiveIndex() == false) {
                allowFiltering = true;
              } else if (prop.getIndexName() != null) {
                allowFiltering |= !isFirstIndex;
                isFirstIndex = false;
              } else {
                allowFiltering = true;
              }
          }
        }
      }
    }

    if (ifFilters != null && !ifFilters.isEmpty()) {
      LOG.error("onlyIf conditions " + ifFilters + " would be ignored in the statement " + select);
    }

    if (allowFiltering) {
      select.allowFiltering();
    }

    return select;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Stream<E> transform(ResultSet resultSet) {
    if (rowMapper != null) {
      return StreamSupport.stream(
              Spliterators.spliteratorUnknownSize(resultSet.iterator(), Spliterator.ORDERED), false)
          .map(rowMapper);
    } else {
      return (Stream<E>)
          StreamSupport.stream(
              Spliterators.spliteratorUnknownSize(resultSet.iterator(), Spliterator.ORDERED),
              false);
    }
  }

  private List<Ordering> getOrCreateOrdering() {
    if (ordering == null) {
      ordering = new ArrayList<Ordering>();
    }
    return ordering;
  }
}
