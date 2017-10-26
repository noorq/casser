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

import static net.helenus.core.Query.eq;

import java.io.Closeable;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import net.helenus.support.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.*;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Table;

import brave.Tracer;
import net.helenus.core.cache.CacheUtil;
import net.helenus.core.cache.Facet;
import net.helenus.core.cache.UnboundFacet;
import net.helenus.core.operation.*;
import net.helenus.core.reflect.Drafted;
import net.helenus.core.reflect.HelenusPropertyNode;
import net.helenus.core.reflect.MapExportable;
import net.helenus.mapping.HelenusEntity;
import net.helenus.mapping.HelenusProperty;
import net.helenus.mapping.MappingUtil;
import net.helenus.mapping.value.*;
import net.helenus.support.Fun.Tuple1;
import net.helenus.support.Fun.Tuple2;
import net.helenus.support.Fun.Tuple6;

public final class HelenusSession extends AbstractSessionOperations implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(HelenusSession.class);
    public static final Object deleted = new Object();

	private final int MAX_CACHE_SIZE = 10000;
	private final int MAX_CACHE_EXPIRE_SECONDS = 600;

	private final Session session;
	private final CodecRegistry registry;
	private final ConsistencyLevel defaultConsistencyLevel;
	private final boolean defaultQueryIdempotency;
	private final MetricRegistry metricRegistry;
	private final Tracer zipkinTracer;
	private final PrintStream printStream;
	private final Class<? extends UnitOfWork> unitOfWorkClass;
	private final SessionRepository sessionRepository;
	private final Executor executor;
	private final boolean dropSchemaOnClose;
	private final Cache<String, Object> sessionCache;
	private final RowColumnValueProvider valueProvider;
	private final StatementColumnValuePreparer valuePreparer;
	private final Metadata metadata;
	private volatile String usingKeyspace;
	private volatile boolean showCql;

	HelenusSession(Session session, String usingKeyspace, CodecRegistry registry, boolean showCql,
			PrintStream printStream, SessionRepositoryBuilder sessionRepositoryBuilder, Executor executor,
			boolean dropSchemaOnClose, ConsistencyLevel consistencyLevel, boolean defaultQueryIdempotency,
			Class<? extends UnitOfWork> unitOfWorkClass, MetricRegistry metricRegistry, Tracer tracer) {
		this.session = session;
		this.registry = registry == null ? CodecRegistry.DEFAULT_INSTANCE : registry;
		this.usingKeyspace = Objects.requireNonNull(usingKeyspace,
				"keyspace needs to be selected before creating session");
		this.showCql = showCql;
		this.printStream = printStream;
		this.sessionRepository = sessionRepositoryBuilder.build();
		this.executor = executor;
		this.dropSchemaOnClose = dropSchemaOnClose;
		this.defaultConsistencyLevel = consistencyLevel;
		this.defaultQueryIdempotency = defaultQueryIdempotency;
		this.unitOfWorkClass = unitOfWorkClass;
		this.metricRegistry = metricRegistry;
		this.zipkinTracer = tracer;

		this.sessionCache = CacheBuilder.newBuilder().maximumSize(MAX_CACHE_SIZE)
				.expireAfterAccess(MAX_CACHE_EXPIRE_SECONDS, TimeUnit.SECONDS)
				.expireAfterWrite(MAX_CACHE_EXPIRE_SECONDS, TimeUnit.SECONDS).recordStats().build();

		this.valueProvider = new RowColumnValueProvider(this.sessionRepository);
		this.valuePreparer = new StatementColumnValuePreparer(this.sessionRepository);
		this.metadata = session.getCluster().getMetadata();
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

	@Override
	public ConsistencyLevel getDefaultConsistencyLevel() {
		return defaultConsistencyLevel;
	}

	@Override
	public boolean getDefaultQueryIdempotency() {
		return defaultQueryIdempotency;
	}

	@Override
	public Object checkCache(String tableName, List<Facet> facets) {
		List<String[]> facetCombinations = CacheUtil.flattenFacets(facets);
		Object result = null;
		for (String[] combination : facetCombinations) {
			String cacheKey = tableName + "." + Arrays.toString(combination);
			result = sessionCache.getIfPresent(cacheKey);
			if (result != null) {
				return result;
			}
		}
		return null;
	}

	@Override
	public void cacheEvict(List<Facet> facets) {
		String tableName = CacheUtil.schemaName(facets);
		List<String[]> facetCombinations = CacheUtil.flattenFacets(facets);
		for (String[] combination : facetCombinations) {
			String cacheKey = tableName + "." + Arrays.toString(combination);
			sessionCache.invalidate(cacheKey);
		}
	}

	@Override
	public void updateCache(Object pojo, List<Facet> facets) {
		Map<String, Object> valueMap = pojo instanceof MapExportable ? ((MapExportable) pojo).toMap() : null;
		List<Facet> boundFacets = new ArrayList<>();
		for (Facet facet : facets) {
			if (facet instanceof UnboundFacet) {
				UnboundFacet unboundFacet = (UnboundFacet) facet;
				UnboundFacet.Binder binder = unboundFacet.binder();
				for (HelenusProperty prop : unboundFacet.getProperties()) {
					Object value;
					if (valueMap == null) {
						value = BeanColumnValueProvider.INSTANCE.getColumnValue(pojo, -1, prop, false);
						if (value != null) {
							binder.setValueForProperty(prop, value.toString());
						}
					} else {
						value = valueMap.get(prop.getPropertyName());
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
        String tableName = CacheUtil.schemaName(facets);
        List<String[]> facetCombinations = CacheUtil.flattenFacets(boundFacets);
		mergeAndUpdateCacheValues(pojo, tableName, facetCombinations);
	}

	@Override
    public void mergeCache(Table<String, String, Either<Object, List<Facet>>> uowCache) {
		List<Either<Object, List<Facet>>> items = uowCache.values().stream().distinct().collect(Collectors.toList());
		for (Either<Object, List<Facet>> item : items) {
		    if (item.isRight()) {
                List<Facet> facets = item.getRight();
                String tableName = CacheUtil.schemaName(facets);
                List<String[]> combinations = CacheUtil.flattenFacets(facets);
                for (String[] combination : combinations) {
                    String cacheKey = tableName + "." + Arrays.toString(combination);
                    sessionCache.invalidate(cacheKey);
                }
            } else {
		        Object pojo = item.getLeft();
                HelenusEntity entity = Helenus.resolve(MappingUtil.getMappingInterface(pojo));
                Map<String, Object> valueMap = pojo instanceof MapExportable ? ((MapExportable) pojo).toMap() : null;
                if (entity.isCacheable()) {
                    List<Facet> boundFacets = new ArrayList<>();
                    for (Facet facet : entity.getFacets()) {
                        if (facet instanceof UnboundFacet) {
                            UnboundFacet unboundFacet = (UnboundFacet) facet;
                            UnboundFacet.Binder binder = unboundFacet.binder();
                            unboundFacet.getProperties().forEach(prop -> {
                                if (valueMap == null) {
                                    Object value = BeanColumnValueProvider.INSTANCE.getColumnValue(pojo, -1, prop,
                                            false);
                                    binder.setValueForProperty(prop, value.toString());
                                } else {
                                    binder.setValueForProperty(prop, valueMap.get(prop.getPropertyName()).toString());
                                }
                            });
                            if (binder.isBound()) {
                                boundFacets.add(binder.bind());
                            }
                        } else {
                            boundFacets.add(facet);
                        }
                    }
                    // NOTE: should equal `String tableName = CacheUtil.schemaName(facets);`
                    List<String[]> facetCombinations = CacheUtil.flattenFacets(boundFacets);
                    String tableName = CacheUtil.schemaName(boundFacets);
                    mergeAndUpdateCacheValues(pojo, tableName, facetCombinations);
                }
            }
		}
	}

	private void mergeAndUpdateCacheValues(Object pojo, String tableName, List<String[]> facetCombinations) {
		Object merged = null;
		for (String[] combination : facetCombinations) {
            String cacheKey = tableName + "." + Arrays.toString(combination);
			Object value = sessionCache.getIfPresent(cacheKey);
			if (value == null) {
				sessionCache.put(cacheKey, pojo);
			} else {
				if (merged == null) {
					merged = pojo;
				} else {
					merged = CacheUtil.merge(value, pojo);
				}
				sessionCache.put(cacheKey, merged);
			}
		}
	}

	public Metadata getMetadata() {
		return metadata;
	}

	public UnitOfWork begin() {
		return this.begin(null);
	}

	public synchronized UnitOfWork begin(UnitOfWork parent) {
		StringBuilder purpose = null;
		if (LOG.isInfoEnabled()) {
			StackTraceElement[] trace = Thread.currentThread().getStackTrace();
			int frame = 2;
			if (trace[2].getMethodName().equals("begin")) {
				frame = 3;
			}
			purpose = new StringBuilder().append(trace[frame].getClassName()).append(".")
					.append(trace[frame].getMethodName()).append("(").append(trace[frame].getFileName()).append(":")
					.append(trace[frame].getLineNumber()).append(")");
		}
		try {
			Class<? extends UnitOfWork> clazz = unitOfWorkClass;
			Constructor<? extends UnitOfWork> ctor = clazz.getConstructor(HelenusSession.class, UnitOfWork.class);
			UnitOfWork uow = ctor.newInstance(this, parent);
			if (LOG.isInfoEnabled() && purpose != null) {
				uow.setPurpose(purpose.toString());
			}
			if (parent != null) {
				parent.addNestedUnitOfWork(uow);
			}
			return uow.begin();
		} catch (NoSuchMethodException | InvocationTargetException | InstantiationException
				| IllegalAccessException e) {
			throw new HelenusException(
					String.format("Unable to instantiate {} as a UnitOfWork.", unitOfWorkClass.getSimpleName()), e);
		}
	}

	public <E> SelectOperation<E> select(E pojo) {
		Objects.requireNonNull(pojo, "supplied object must be a dsl for a registered entity but cannot be null");
		ColumnValueProvider valueProvider = getValueProvider();
		HelenusEntity entity = Helenus.resolve(pojo);
		Class<?> entityClass = entity.getMappingInterface();

		return new SelectOperation<E>(this, entity, (r) -> {
			Map<String, Object> map = new ValueProviderMap(r, valueProvider, entity);
			return (E) Helenus.map(entityClass, map);
		});
	}

	public <E> SelectOperation<E> select(Class<E> entityClass) {
		Objects.requireNonNull(entityClass, "entityClass is empty");
		ColumnValueProvider valueProvider = getValueProvider();
		HelenusEntity entity = Helenus.entity(entityClass);

		return new SelectOperation<E>(this, entity, (r) -> {
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
		return new SelectOperation<Tuple1<V1>>(this, new Mappers.Mapper1<V1>(getValueProvider(), p1), p1);
	}

	public <V1, V2> SelectOperation<Tuple2<V1, V2>> select(Getter<V1> getter1, Getter<V2> getter2) {
		Objects.requireNonNull(getter1, "field 1 is empty");
		Objects.requireNonNull(getter2, "field 2 is empty");

		HelenusPropertyNode p1 = MappingUtil.resolveMappingProperty(getter1);
		HelenusPropertyNode p2 = MappingUtil.resolveMappingProperty(getter2);
		return new SelectOperation<Fun.Tuple2<V1, V2>>(this, new Mappers.Mapper2<V1, V2>(getValueProvider(), p1, p2),
				p1, p2);
	}

	public <V1, V2, V3> SelectOperation<Fun.Tuple3<V1, V2, V3>> select(Getter<V1> getter1, Getter<V2> getter2,
			Getter<V3> getter3) {
		Objects.requireNonNull(getter1, "field 1 is empty");
		Objects.requireNonNull(getter2, "field 2 is empty");
		Objects.requireNonNull(getter3, "field 3 is empty");

		HelenusPropertyNode p1 = MappingUtil.resolveMappingProperty(getter1);
		HelenusPropertyNode p2 = MappingUtil.resolveMappingProperty(getter2);
		HelenusPropertyNode p3 = MappingUtil.resolveMappingProperty(getter3);
		return new SelectOperation<Fun.Tuple3<V1, V2, V3>>(this,
				new Mappers.Mapper3<V1, V2, V3>(getValueProvider(), p1, p2, p3), p1, p2, p3);
	}

	public <V1, V2, V3, V4> SelectOperation<Fun.Tuple4<V1, V2, V3, V4>> select(Getter<V1> getter1, Getter<V2> getter2,
			Getter<V3> getter3, Getter<V4> getter4) {
		Objects.requireNonNull(getter1, "field 1 is empty");
		Objects.requireNonNull(getter2, "field 2 is empty");
		Objects.requireNonNull(getter3, "field 3 is empty");
		Objects.requireNonNull(getter4, "field 4 is empty");

		HelenusPropertyNode p1 = MappingUtil.resolveMappingProperty(getter1);
		HelenusPropertyNode p2 = MappingUtil.resolveMappingProperty(getter2);
		HelenusPropertyNode p3 = MappingUtil.resolveMappingProperty(getter3);
		HelenusPropertyNode p4 = MappingUtil.resolveMappingProperty(getter4);
		return new SelectOperation<Fun.Tuple4<V1, V2, V3, V4>>(this,
				new Mappers.Mapper4<V1, V2, V3, V4>(getValueProvider(), p1, p2, p3, p4), p1, p2, p3, p4);
	}

	public <V1, V2, V3, V4, V5> SelectOperation<Fun.Tuple5<V1, V2, V3, V4, V5>> select(Getter<V1> getter1,
			Getter<V2> getter2, Getter<V3> getter3, Getter<V4> getter4, Getter<V5> getter5) {
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
		return new SelectOperation<Fun.Tuple5<V1, V2, V3, V4, V5>>(this,
				new Mappers.Mapper5<V1, V2, V3, V4, V5>(getValueProvider(), p1, p2, p3, p4, p5), p1, p2, p3, p4, p5);
	}

	public <V1, V2, V3, V4, V5, V6> SelectOperation<Fun.Tuple6<V1, V2, V3, V4, V5, V6>> select(Getter<V1> getter1,
			Getter<V2> getter2, Getter<V3> getter3, Getter<V4> getter4, Getter<V5> getter5, Getter<V6> getter6) {
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
		return new SelectOperation<Tuple6<V1, V2, V3, V4, V5, V6>>(this,
				new Mappers.Mapper6<V1, V2, V3, V4, V5, V6>(getValueProvider(), p1, p2, p3, p4, p5, p6), p1, p2, p3, p4,
				p5, p6);
	}

	public <V1, V2, V3, V4, V5, V6, V7> SelectOperation<Fun.Tuple7<V1, V2, V3, V4, V5, V6, V7>> select(
			Getter<V1> getter1, Getter<V2> getter2, Getter<V3> getter3, Getter<V4> getter4, Getter<V5> getter5,
			Getter<V6> getter6, Getter<V7> getter7) {
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
		return new SelectOperation<Fun.Tuple7<V1, V2, V3, V4, V5, V6, V7>>(this,
				new Mappers.Mapper7<V1, V2, V3, V4, V5, V6, V7>(getValueProvider(), p1, p2, p3, p4, p5, p6, p7), p1, p2,
				p3, p4, p5, p6, p7);
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

	public <E> UpdateOperation<E> update(Drafted<E> drafted) {
		if (drafted instanceof AbstractEntityDraft == false) {
			throw new HelenusMappingException(
					"update of draft objects that don't inherit from AbstractEntityDraft is not yet supported");
		}
		AbstractEntityDraft<E> draft = (AbstractEntityDraft<E>) drafted;
		UpdateOperation update = new UpdateOperation<E>(this, draft);
		Map<String, Object> map = draft.toMap();
		Set<String> mutatedProperties = draft.mutated();
		HelenusEntity entity = Helenus.entity(draft.getEntityClass());

		// Add all the mutated values contained in the draft.
		entity.getOrderedProperties().forEach(property -> {
			switch (property.getColumnType()) {
				case PARTITION_KEY :
				case CLUSTERING_COLUMN :
					break;
				default :
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

		// Add the partition and clustering keys if they were in the draft (normally the
		// case).
		entity.getOrderedProperties().forEach(property -> {
			switch (property.getColumnType()) {
				case PARTITION_KEY :
				case CLUSTERING_COLUMN :
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
		Objects.requireNonNull(pojo,
				"supplied object must be either an instance of the entity class or a dsl for it, but cannot be null");
		HelenusEntity entity = null;
		try {
			entity = Helenus.resolve(pojo);
		} catch (HelenusMappingException e) {
		}
		if (entity != null) {
			return new InsertOperation<T>(this, entity.getMappingInterface(), true);
		} else {
			return this.<T>insert(pojo, null);
		}
	}

	public <T> InsertOperation<T> insert(Drafted draft) {
		return insert(draft.build(), draft.mutated());
	}

	private <T> InsertOperation<T> insert(T pojo, Set<String> mutations) {
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
		Objects.requireNonNull(pojo,
				"supplied object must be either an instance of the entity class or a dsl for it, but cannot be null");
		HelenusEntity entity = null;
		try {
			entity = Helenus.resolve(pojo);
		} catch (HelenusMappingException e) {
		}
		if (entity != null) {
			return new InsertOperation<T>(this, entity.getMappingInterface(), false);
		} else {
			return this.<T>upsert(pojo, null);
		}
	}

	private <T> InsertOperation<T> upsert(T pojo, Set<String> mutations) {
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
			case TABLE :
				execute(SchemaUtil.dropTable(entity), true);
				break;

			case UDT :
				execute(SchemaUtil.dropUserType(entity), true);
				break;
		}
	}
}
