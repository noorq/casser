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
package net.helenus.core;

import brave.Tracer;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.*;
import net.helenus.core.operation.*;
import net.helenus.core.reflect.Drafted;
import net.helenus.core.reflect.HelenusPropertyNode;
import net.helenus.mapping.HelenusEntity;
import net.helenus.mapping.MappingUtil;
import net.helenus.mapping.value.*;
import net.helenus.support.DslPropertyException;
import net.helenus.support.Fun;
import net.helenus.support.Fun.Tuple1;
import net.helenus.support.Fun.Tuple2;
import net.helenus.support.Fun.Tuple6;
import net.helenus.support.HelenusMappingException;

import java.io.Closeable;
import java.io.PrintStream;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static net.helenus.core.Query.eq;

public final class HelenusSession extends AbstractSessionOperations implements Closeable {

  private final int MAX_CACHE_SIZE = 10000;
  private final int MAX_CACHE_EXPIRE_SECONDS = 600;

  private final Session session;
  private final CodecRegistry registry;
  private volatile String usingKeyspace;
  private volatile boolean showCql;
  private final ConsistencyLevel defaultConsistencyLevel;
  private final MetricRegistry metricRegistry;
  private final Tracer zipkinTracer;
  private final PrintStream printStream;
  private final SessionRepository sessionRepository;
  private final Executor executor;
  private final boolean dropSchemaOnClose;

  private final RowColumnValueProvider valueProvider;
  private final StatementColumnValuePreparer valuePreparer;
  private final Metadata metadata;
  private final SessionCache sessionCache;

  HelenusSession(
          Session session,
          String usingKeyspace,
          CodecRegistry registry,
          boolean showCql,
          PrintStream printStream,
          SessionRepositoryBuilder sessionRepositoryBuilder,
          Executor executor,
          boolean dropSchemaOnClose,
          ConsistencyLevel consistencyLevel,
          MetricRegistry metricRegistry,
          Tracer tracer) {
    this.session = session;
    this.registry = registry == null ? CodecRegistry.DEFAULT_INSTANCE : registry;
    this.usingKeyspace =
            Objects.requireNonNull(
                    usingKeyspace, "keyspace needs to be selected before creating session");
    this.showCql = showCql;
    this.printStream = printStream;
    this.sessionRepository = sessionRepositoryBuilder.build();
    this.executor = executor;
    this.dropSchemaOnClose = dropSchemaOnClose;
    this.defaultConsistencyLevel = consistencyLevel;
    this.metricRegistry = metricRegistry;
    this.zipkinTracer = tracer;

    this.valueProvider = new RowColumnValueProvider(this.sessionRepository);
    this.valuePreparer = new StatementColumnValuePreparer(this.sessionRepository);
    this.metadata = session.getCluster().getMetadata();
    this.sessionCache = new SessionCache();
  }

  @Override
  public Session currentSession() {
    return session;
  }

  @Override
  public String usingKeyspace() {
    return usingKeyspace;
  }

  public HelenusSession useKeyspace(String keyspace) {
    session.execute(SchemaUtil.use(keyspace, false));
    this.usingKeyspace = keyspace;
    return this;
  }

  @Override
  public boolean isShowCql() {
    return showCql;
  }

  @Override
  public PrintStream getPrintStream() {
    return printStream;
  }

  public HelenusSession showCql() {
    this.showCql = true;
    return this;
  }

  public HelenusSession showCql(boolean showCql) {
    this.showCql = showCql;
    return this;
  }

  @Override
  public Executor getExecutor() {
    return executor;
  }

  @Override
  public SessionRepository getSessionRepository() {
    return sessionRepository;
  }

  @Override
  public ColumnValueProvider getValueProvider() {
    return valueProvider;
  }

  @Override
  public ColumnValuePreparer getValuePreparer() {
    return valuePreparer;
  }

  @Override
  public Tracer getZipkinTracer() {
    return zipkinTracer;
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  public ConsistencyLevel getDefaultConsistencyLevel() {
    return defaultConsistencyLevel;
  }

  public Metadata getMetadata() {
    return metadata;
  }

  public synchronized UnitOfWork begin() {
    return new UnitOfWork(this, null).begin();
  }

  public synchronized UnitOfWork begin(UnitOfWork parent) {
    UnitOfWork child = new UnitOfWork(this, parent);
    parent.addNestedUnitOfWork(child);
    return child.begin();
  }

  public <E> SelectOperation<E> select(E pojo) {
    Objects.requireNonNull(pojo, "supplied object must be a dsl for a registered entity but cannot be null");
    ColumnValueProvider valueProvider = getValueProvider();
    HelenusEntity entity = Helenus.resolve(pojo);
    Class<?> entityClass = entity.getMappingInterface();

    return new SelectOperation<E>(
            this,
            entity,
            (r) -> {
              Map<String, Object> map = new ValueProviderMap(r, valueProvider, entity);
              return (E) Helenus.map(entityClass, map);
            });
  }

  public <E> SelectOperation<E> select(Class<E> entityClass) {
    Objects.requireNonNull(entityClass, "entityClass is empty");
    ColumnValueProvider valueProvider = getValueProvider();
    HelenusEntity entity = Helenus.entity(entityClass);

    return new SelectOperation<E>(
            this,
            entity,
            (r) -> {
              Map<String, Object> map = new ValueProviderMap(r, valueProvider, entity);
              return (E) Helenus.map(entityClass, map);
            });
  }

  public SelectOperation<Fun.ArrayTuple> select() {
    return new SelectOperation<Fun.ArrayTuple>(this);
  }

  public SelectOperation<Row> selectAll(Class<?> entityClass) {
    Objects.requireNonNull(entityClass, "entityClass is empty");
    return new SelectOperation<Row>(this, Helenus.entity(entityClass));
  }

  public <E> SelectOperation<Row> selectAll(E pojo) {
    Objects.requireNonNull(pojo, "supplied object must be a dsl for a registered entity but cannot be null");
    HelenusEntity entity = Helenus.resolve(pojo);
    return new SelectOperation<Row>(this, entity);
  }

  public <E> SelectOperation<E> selectAll(Class<E> entityClass, Function<Row, E> rowMapper) {
    Objects.requireNonNull(entityClass, "entityClass is empty");
    Objects.requireNonNull(rowMapper, "rowMapper is empty");
    return new SelectOperation<E>(this, Helenus.entity(entityClass), rowMapper);
  }

  public <V1> SelectOperation<Fun.Tuple1<V1>> select(Getter<V1> getter1) {
    Objects.requireNonNull(getter1, "field 1 is empty");

    HelenusPropertyNode p1 = MappingUtil.resolveMappingProperty(getter1);
    return new SelectOperation<Tuple1<V1>>(
        this, new Mappers.Mapper1<V1>(getValueProvider(), p1), p1);
  }

  public <V1, V2> SelectOperation<Tuple2<V1, V2>> select(Getter<V1> getter1, Getter<V2> getter2) {
    Objects.requireNonNull(getter1, "field 1 is empty");
    Objects.requireNonNull(getter2, "field 2 is empty");

    HelenusPropertyNode p1 = MappingUtil.resolveMappingProperty(getter1);
    HelenusPropertyNode p2 = MappingUtil.resolveMappingProperty(getter2);
    return new SelectOperation<Fun.Tuple2<V1, V2>>(
        this, new Mappers.Mapper2<V1, V2>(getValueProvider(), p1, p2), p1, p2);
  }

  public <V1, V2, V3> SelectOperation<Fun.Tuple3<V1, V2, V3>> select(
      Getter<V1> getter1, Getter<V2> getter2, Getter<V3> getter3) {
    Objects.requireNonNull(getter1, "field 1 is empty");
    Objects.requireNonNull(getter2, "field 2 is empty");
    Objects.requireNonNull(getter3, "field 3 is empty");

    HelenusPropertyNode p1 = MappingUtil.resolveMappingProperty(getter1);
    HelenusPropertyNode p2 = MappingUtil.resolveMappingProperty(getter2);
    HelenusPropertyNode p3 = MappingUtil.resolveMappingProperty(getter3);
    return new SelectOperation<Fun.Tuple3<V1, V2, V3>>(
        this, new Mappers.Mapper3<V1, V2, V3>(getValueProvider(), p1, p2, p3), p1, p2, p3);
  }

  public <V1, V2, V3, V4> SelectOperation<Fun.Tuple4<V1, V2, V3, V4>> select(
      Getter<V1> getter1, Getter<V2> getter2, Getter<V3> getter3, Getter<V4> getter4) {
    Objects.requireNonNull(getter1, "field 1 is empty");
    Objects.requireNonNull(getter2, "field 2 is empty");
    Objects.requireNonNull(getter3, "field 3 is empty");
    Objects.requireNonNull(getter4, "field 4 is empty");

    HelenusPropertyNode p1 = MappingUtil.resolveMappingProperty(getter1);
    HelenusPropertyNode p2 = MappingUtil.resolveMappingProperty(getter2);
    HelenusPropertyNode p3 = MappingUtil.resolveMappingProperty(getter3);
    HelenusPropertyNode p4 = MappingUtil.resolveMappingProperty(getter4);
    return new SelectOperation<Fun.Tuple4<V1, V2, V3, V4>>(
        this,
        new Mappers.Mapper4<V1, V2, V3, V4>(getValueProvider(), p1, p2, p3, p4),
        p1,
        p2,
        p3,
        p4);
  }

  public <V1, V2, V3, V4, V5> SelectOperation<Fun.Tuple5<V1, V2, V3, V4, V5>> select(
      Getter<V1> getter1,
      Getter<V2> getter2,
      Getter<V3> getter3,
      Getter<V4> getter4,
      Getter<V5> getter5) {
    Objects.requireNonNull(getter1, "field 1 is empty");
    Objects.requireNonNull(getter2, "field 2 is empty");
    Objects.requireNonNull(getter3, "field 3 is empty");
    Objects.requireNonNull(getter4, "field 4 is empty");
    Objects.requireNonNull(getter5, "field 5 is empty");

    HelenusPropertyNode p1 = MappingUtil.resolveMappingProperty(getter1);
    HelenusPropertyNode p2 = MappingUtil.resolveMappingProperty(getter2);
    HelenusPropertyNode p3 = MappingUtil.resolveMappingProperty(getter3);
    HelenusPropertyNode p4 = MappingUtil.resolveMappingProperty(getter4);
    HelenusPropertyNode p5 = MappingUtil.resolveMappingProperty(getter5);
    return new SelectOperation<Fun.Tuple5<V1, V2, V3, V4, V5>>(
        this,
        new Mappers.Mapper5<V1, V2, V3, V4, V5>(getValueProvider(), p1, p2, p3, p4, p5),
        p1,
        p2,
        p3,
        p4,
        p5);
  }

  public <V1, V2, V3, V4, V5, V6> SelectOperation<Fun.Tuple6<V1, V2, V3, V4, V5, V6>> select(
      Getter<V1> getter1,
      Getter<V2> getter2,
      Getter<V3> getter3,
      Getter<V4> getter4,
      Getter<V5> getter5,
      Getter<V6> getter6) {
    Objects.requireNonNull(getter1, "field 1 is empty");
    Objects.requireNonNull(getter2, "field 2 is empty");
    Objects.requireNonNull(getter3, "field 3 is empty");
    Objects.requireNonNull(getter4, "field 4 is empty");
    Objects.requireNonNull(getter5, "field 5 is empty");
    Objects.requireNonNull(getter6, "field 6 is empty");

    HelenusPropertyNode p1 = MappingUtil.resolveMappingProperty(getter1);
    HelenusPropertyNode p2 = MappingUtil.resolveMappingProperty(getter2);
    HelenusPropertyNode p3 = MappingUtil.resolveMappingProperty(getter3);
    HelenusPropertyNode p4 = MappingUtil.resolveMappingProperty(getter4);
    HelenusPropertyNode p5 = MappingUtil.resolveMappingProperty(getter5);
    HelenusPropertyNode p6 = MappingUtil.resolveMappingProperty(getter6);
    return new SelectOperation<Tuple6<V1, V2, V3, V4, V5, V6>>(
        this,
        new Mappers.Mapper6<V1, V2, V3, V4, V5, V6>(getValueProvider(), p1, p2, p3, p4, p5, p6),
        p1,
        p2,
        p3,
        p4,
        p5,
        p6);
  }

  public <V1, V2, V3, V4, V5, V6, V7>
      SelectOperation<Fun.Tuple7<V1, V2, V3, V4, V5, V6, V7>> select(
          Getter<V1> getter1,
          Getter<V2> getter2,
          Getter<V3> getter3,
          Getter<V4> getter4,
          Getter<V5> getter5,
          Getter<V6> getter6,
          Getter<V7> getter7) {
    Objects.requireNonNull(getter1, "field 1 is empty");
    Objects.requireNonNull(getter2, "field 2 is empty");
    Objects.requireNonNull(getter3, "field 3 is empty");
    Objects.requireNonNull(getter4, "field 4 is empty");
    Objects.requireNonNull(getter5, "field 5 is empty");
    Objects.requireNonNull(getter6, "field 6 is empty");
    Objects.requireNonNull(getter7, "field 7 is empty");

    HelenusPropertyNode p1 = MappingUtil.resolveMappingProperty(getter1);
    HelenusPropertyNode p2 = MappingUtil.resolveMappingProperty(getter2);
    HelenusPropertyNode p3 = MappingUtil.resolveMappingProperty(getter3);
    HelenusPropertyNode p4 = MappingUtil.resolveMappingProperty(getter4);
    HelenusPropertyNode p5 = MappingUtil.resolveMappingProperty(getter5);
    HelenusPropertyNode p6 = MappingUtil.resolveMappingProperty(getter6);
    HelenusPropertyNode p7 = MappingUtil.resolveMappingProperty(getter7);
    return new SelectOperation<Fun.Tuple7<V1, V2, V3, V4, V5, V6, V7>>(
        this,
        new Mappers.Mapper7<V1, V2, V3, V4, V5, V6, V7>(
            getValueProvider(), p1, p2, p3, p4, p5, p6, p7),
        p1,
        p2,
        p3,
        p4,
        p5,
        p6,
        p7);
  }

  public CountOperation count() {
    return new CountOperation(this);
  }

  public CountOperation count(Object dsl) {
    Objects.requireNonNull(dsl, "dsl is empty");
    return new CountOperation(this, Helenus.resolve(dsl));
  }

  public UpdateOperation<ResultSet> update() {
    return new UpdateOperation<ResultSet>(this);
  }

  public <T> UpdateOperation<T> update(Drafted<T> draft) {
    UpdateOperation update = new UpdateOperation<T>(this, draft.build());
    Map<String, Object> map = draft.toMap();
    HelenusEntity entity = draft.getEntity();
    Set<String> mutatedProperties = draft.mutated();

    // Add all the mutated values contained in the draft.
    entity.getOrderedProperties().forEach(property -> {
      switch (property.getColumnType()) {
        case PARTITION_KEY:
        case CLUSTERING_COLUMN:
          break;
        default:
          String propertyName = property.getPropertyName();
          if (mutatedProperties.contains(propertyName)) {
            Object value = map.get(propertyName);
            Getter<Object> getter = new Getter<Object>() {
              @Override
              public Object get() {
                throw new DslPropertyException(new HelenusPropertyNode(property, Optional.empty()));
              }
            };
            update.set(getter, value);
          }
      }
    });

    // Add the partition and clustering keys if they were in the draft (normally the case).
    entity.getOrderedProperties().forEach(property -> {
      switch (property.getColumnType()) {
        case PARTITION_KEY:
        case CLUSTERING_COLUMN:
          String propertyName = property.getPropertyName();
          Object value = map.get(propertyName);
          Getter<Object> getter = new Getter<Object>() {
            @Override
            public Object get() {
              throw new DslPropertyException(new HelenusPropertyNode(property, Optional.empty()));
            }
          };
          update.where(getter, eq(value));
      }
    });

    return update;
  }

  public <V> UpdateOperation<ResultSet> update(Getter<V> getter, V v) {
    Objects.requireNonNull(getter, "field is empty");
    Objects.requireNonNull(v, "value is empty");

    HelenusPropertyNode p = MappingUtil.resolveMappingProperty(getter);

    return new UpdateOperation<ResultSet>(this, p, v);
  }

  public InsertOperation<ResultSet> insert() {
    return new InsertOperation<ResultSet>(this, true);
  }

  public <T> InsertOperation<T> insert(Class<?> resultType) {
    return new InsertOperation<T>(this, resultType, true);
  }

  public <T> InsertOperation<T> insert(T pojo) {
    Objects.requireNonNull(pojo, "supplied object must be either an instance of the entity class or a dsl for it, but cannot be null");
    HelenusEntity entity = null;
    try { entity = Helenus.resolve(pojo); } catch (HelenusMappingException e) {}
    if (entity != null) {
      return new InsertOperation<T>(this, entity.getMappingInterface(), true);
    } else {
      return this.<T>insert(pojo, null);
    }
  }

  public <T> InsertOperation<T> insert(Drafted draft) {
    return this.<T>insert((T) draft.build(), draft.mutated());
  }

  public <T> InsertOperation<T> insert(T pojo, Set<String> mutations) {
    Objects.requireNonNull(pojo, "pojo is empty");

    Class<?> iface = MappingUtil.getMappingInterface(pojo);
    HelenusEntity entity = Helenus.entity(iface);

    return new InsertOperation<T>(this, entity, pojo, mutations, true);
  }

  public InsertOperation<ResultSet> upsert() {
    return new InsertOperation<ResultSet>(this, false);
  }

  public <T> InsertOperation<T> upsert(Class<?> resultType) {
    return new InsertOperation<T>(this, resultType, false);
  }

  public <T> InsertOperation<T> upsert(Drafted draft) {
    return this.<T>upsert((T) draft.build(), draft.mutated());
  }

  public <T> InsertOperation<T> upsert(T pojo) {
    Objects.requireNonNull(pojo, "supplied object must be either an instance of the entity class or a dsl for it, but cannot be null");
    HelenusEntity entity = null;
    try { entity = Helenus.resolve(pojo); } catch (HelenusMappingException e) {}
    if (entity != null) {
      return new InsertOperation<T>(this, entity.getMappingInterface(), false);
    } else {
      return this.<T>upsert(pojo, null);
    }
  }

  public <T> InsertOperation<T> upsert(T pojo, Set<String> mutations) {
    Objects.requireNonNull(pojo, "pojo is empty");

    Class<?> iface = MappingUtil.getMappingInterface(pojo);
    HelenusEntity entity = Helenus.entity(iface);

    return new InsertOperation<T>(this, entity, pojo, mutations, false);
  }

  public DeleteOperation delete() {
    return new DeleteOperation(this);
  }

  public DeleteOperation delete(Object dsl) {
    Objects.requireNonNull(dsl, "dsl is empty");
    return new DeleteOperation(this, Helenus.resolve(dsl));
  }

  public Session getSession() {
    return session;
  }

  public <E> E dsl(Class<E> iface) {
    return Helenus.dsl(iface, getMetadata());
  }

  public void close() {

    if (session.isClosed()) {
      return;
    }

    if (dropSchemaOnClose) {
      dropSchema();
    }

    session.close();
  }

  public CloseFuture closeAsync() {

    if (!session.isClosed() && dropSchemaOnClose) {
      dropSchema();
    }

    return session.closeAsync();
  }

  private void dropSchema() {

    sessionRepository.entities().forEach(e -> dropEntity(e));
  }

  private void dropEntity(HelenusEntity entity) {

    switch (entity.getType()) {
      case TABLE:
        execute(SchemaUtil.dropTable(entity), true);
        break;

      case UDT:
        execute(SchemaUtil.dropUserType(entity), true);
        break;
    }
  }

  public SessionCache getSessionCache() { return sessionCache; }

}
