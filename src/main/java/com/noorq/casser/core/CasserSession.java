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
package com.noorq.casser.core;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.Session;
import com.noorq.casser.core.operation.CountOperation;
import com.noorq.casser.core.operation.DeleteOperation;
import com.noorq.casser.core.operation.InsertOperation;
import com.noorq.casser.core.operation.SelectOperation;
import com.noorq.casser.core.operation.UpdateOperation;
import com.noorq.casser.core.reflect.CasserPropertyNode;
import com.noorq.casser.core.tuple.Tuple1;
import com.noorq.casser.core.tuple.Tuple2;
import com.noorq.casser.core.tuple.Tuple3;
import com.noorq.casser.core.tuple.Tuple4;
import com.noorq.casser.core.tuple.Tuple5;
import com.noorq.casser.core.tuple.Tuple6;
import com.noorq.casser.core.tuple.Tuple7;
import com.noorq.casser.mapping.CasserMappingEntity;
import com.noorq.casser.mapping.CasserMappingRepository;
import com.noorq.casser.mapping.MappingRepositoryBuilder;
import com.noorq.casser.mapping.MappingUtil;
import com.noorq.casser.mapping.map.RowProviderMap;
import com.noorq.casser.mapping.value.ColumnValuePreparer;
import com.noorq.casser.mapping.value.ColumnValueProvider;
import com.noorq.casser.mapping.value.RowColumnValueProvider;
import com.noorq.casser.mapping.value.StatementColumnValuePreparer;

public class CasserSession extends AbstractSessionOperations implements Closeable {

	private final Session session;
	private volatile String usingKeyspace;
	private volatile boolean showCql;
	private final CasserMappingRepository mappingRepository;
	private final Executor executor;
	private final boolean dropSchemaOnClose;
	
	private final RowColumnValueProvider valueProvider;
	private final StatementColumnValuePreparer valuePreparer;
	
	CasserSession(Session session,
			String usingKeyspace,
			boolean showCql, 
			MappingRepositoryBuilder mappingRepositoryBuilder, 
			Executor executor,
			boolean dropSchemaOnClose) {
		this.session = session;
		this.usingKeyspace = Objects.requireNonNull(usingKeyspace, "keyspace needs to be selected before creating session");
		this.showCql = showCql;
		this.mappingRepository = mappingRepositoryBuilder.build();
		this.executor = executor;
		this.dropSchemaOnClose = dropSchemaOnClose;
		
		this.valueProvider = new RowColumnValueProvider(this.mappingRepository);
		this.valuePreparer = new StatementColumnValuePreparer(this.mappingRepository);
	}
	
	@Override
	public Session currentSession() {
		return session;
	}
	
	@Override
	public String usingKeyspace() {
		return usingKeyspace;
	}
	
	@Override
	public boolean isShowCql() {
		return showCql;
	}

	public CasserSession showCql() {
		this.showCql = true;
		return this;
	}
	
	public CasserSession showCql(boolean showCql) {
		this.showCql = showCql;
		return this;
	}
	
	@Override
	public Executor getExecutor() {
		return executor;
	}
	
	@Override
	public ColumnValueProvider getValueProvider() {
		return valueProvider;
	}
	
	@Override
	public ColumnValuePreparer getValuePreparer() {
		return valuePreparer;
	}

	public <E> SelectOperation<E> select(Class<E> entityClass) {
		Objects.requireNonNull(entityClass, "entityClass is empty");
		
		Class<?> iface = MappingUtil.getMappingInterface(entityClass);
		CasserMappingEntity entity = mappingRepository.getEntity(iface);
		
		List<CasserPropertyNode> props = entity.getMappingProperties()
		.stream()
		.map(p -> new CasserPropertyNode(p, Optional.empty()))
		.collect(Collectors.toList());
		
		ColumnValueProvider valueProvider = getValueProvider();
		
		return new SelectOperation<E>(this, (r) -> {
			Map<String, Object> map = new RowProviderMap(r, valueProvider, entity);
			return (E) Casser.map(iface, map);
			
		}, props.toArray(new CasserPropertyNode[props.size()]));
	}
	
	public <V1> SelectOperation<Tuple1<V1>> select(Getter<V1> getter1) {
		Objects.requireNonNull(getter1, "field 1 is empty");
		
		CasserPropertyNode p1 = MappingUtil.resolveMappingProperty(getter1);
		return new SelectOperation<Tuple1<V1>>(this, new Tuple1.Mapper<V1>(getValueProvider(), p1), p1);
	}

	public <V1, V2> SelectOperation<Tuple2<V1, V2>> select(Getter<V1> getter1, Getter<V2> getter2) {
		Objects.requireNonNull(getter1, "field 1 is empty");
		Objects.requireNonNull(getter2, "field 2 is empty");
		
		CasserPropertyNode p1 = MappingUtil.resolveMappingProperty(getter1);
		CasserPropertyNode p2 = MappingUtil.resolveMappingProperty(getter2);
		return new SelectOperation<Tuple2<V1, V2>>(this, new Tuple2.Mapper<V1, V2>(getValueProvider(), p1, p2), p1, p2);
	}

	public <V1, V2, V3> SelectOperation<Tuple3<V1, V2, V3>> select(Getter<V1> getter1, Getter<V2> getter2, Getter<V3> getter3) {
		Objects.requireNonNull(getter1, "field 1 is empty");
		Objects.requireNonNull(getter2, "field 2 is empty");
		Objects.requireNonNull(getter3, "field 3 is empty");
		
		CasserPropertyNode p1 = MappingUtil.resolveMappingProperty(getter1);
		CasserPropertyNode p2 = MappingUtil.resolveMappingProperty(getter2);
		CasserPropertyNode p3 = MappingUtil.resolveMappingProperty(getter3);
		return new SelectOperation<Tuple3<V1, V2, V3>>(this, new Tuple3.Mapper<V1, V2, V3>(getValueProvider(), p1, p2, p3), p1, p2, p3);
	}

	public <V1, V2, V3, V4> SelectOperation<Tuple4<V1, V2, V3, V4>> select(
			Getter<V1> getter1, Getter<V2> getter2, Getter<V3> getter3, Getter<V4> getter4) {
		Objects.requireNonNull(getter1, "field 1 is empty");
		Objects.requireNonNull(getter2, "field 2 is empty");
		Objects.requireNonNull(getter3, "field 3 is empty");
		Objects.requireNonNull(getter4, "field 4 is empty");
		
		CasserPropertyNode p1 = MappingUtil.resolveMappingProperty(getter1);
		CasserPropertyNode p2 = MappingUtil.resolveMappingProperty(getter2);
		CasserPropertyNode p3 = MappingUtil.resolveMappingProperty(getter3);
		CasserPropertyNode p4 = MappingUtil.resolveMappingProperty(getter4);
		return new SelectOperation<Tuple4<V1, V2, V3, V4>>(this, new Tuple4.Mapper<V1, V2, V3, V4>(getValueProvider(), p1, p2, p3, p4), p1, p2, p3, p4);
	}

	public <V1, V2, V3, V4, V5> SelectOperation<Tuple5<V1, V2, V3, V4, V5>> select(
			Getter<V1> getter1, Getter<V2> getter2, Getter<V3> getter3, Getter<V4> getter4, Getter<V5> getter5) {
		Objects.requireNonNull(getter1, "field 1 is empty");
		Objects.requireNonNull(getter2, "field 2 is empty");
		Objects.requireNonNull(getter3, "field 3 is empty");
		Objects.requireNonNull(getter4, "field 4 is empty");
		Objects.requireNonNull(getter5, "field 5 is empty");
		
		CasserPropertyNode p1 = MappingUtil.resolveMappingProperty(getter1);
		CasserPropertyNode p2 = MappingUtil.resolveMappingProperty(getter2);
		CasserPropertyNode p3 = MappingUtil.resolveMappingProperty(getter3);
		CasserPropertyNode p4 = MappingUtil.resolveMappingProperty(getter4);
		CasserPropertyNode p5 = MappingUtil.resolveMappingProperty(getter5);
		return new SelectOperation<Tuple5<V1, V2, V3, V4, V5>>(this, 
				new Tuple5.Mapper<V1, V2, V3, V4, V5>(getValueProvider(), p1, p2, p3, p4, p5), 
				p1, p2, p3, p4, p5);
	}
	
	public <V1, V2, V3, V4, V5, V6> SelectOperation<Tuple6<V1, V2, V3, V4, V5, V6>> select(
			Getter<V1> getter1, Getter<V2> getter2, Getter<V3> getter3, 
			Getter<V4> getter4, Getter<V5> getter5, Getter<V6> getter6) {
		Objects.requireNonNull(getter1, "field 1 is empty");
		Objects.requireNonNull(getter2, "field 2 is empty");
		Objects.requireNonNull(getter3, "field 3 is empty");
		Objects.requireNonNull(getter4, "field 4 is empty");
		Objects.requireNonNull(getter5, "field 5 is empty");
		Objects.requireNonNull(getter6, "field 6 is empty");
		
		CasserPropertyNode p1 = MappingUtil.resolveMappingProperty(getter1);
		CasserPropertyNode p2 = MappingUtil.resolveMappingProperty(getter2);
		CasserPropertyNode p3 = MappingUtil.resolveMappingProperty(getter3);
		CasserPropertyNode p4 = MappingUtil.resolveMappingProperty(getter4);
		CasserPropertyNode p5 = MappingUtil.resolveMappingProperty(getter5);
		CasserPropertyNode p6 = MappingUtil.resolveMappingProperty(getter6);
		return new SelectOperation<Tuple6<V1, V2, V3, V4, V5, V6>>(this, 
				new Tuple6.Mapper<V1, V2, V3, V4, V5, V6>(getValueProvider(), p1, p2, p3, p4, p5, p6), 
				p1, p2, p3, p4, p5, p6);
	}

	public <V1, V2, V3, V4, V5, V6, V7> SelectOperation<Tuple7<V1, V2, V3, V4, V5, V6, V7>> select(
			Getter<V1> getter1, Getter<V2> getter2, Getter<V3> getter3, 
			Getter<V4> getter4, Getter<V5> getter5, Getter<V6> getter6,
			Getter<V7> getter7) {
		Objects.requireNonNull(getter1, "field 1 is empty");
		Objects.requireNonNull(getter2, "field 2 is empty");
		Objects.requireNonNull(getter3, "field 3 is empty");
		Objects.requireNonNull(getter4, "field 4 is empty");
		Objects.requireNonNull(getter5, "field 5 is empty");
		Objects.requireNonNull(getter6, "field 6 is empty");
		Objects.requireNonNull(getter7, "field 7 is empty");
		
		CasserPropertyNode p1 = MappingUtil.resolveMappingProperty(getter1);
		CasserPropertyNode p2 = MappingUtil.resolveMappingProperty(getter2);
		CasserPropertyNode p3 = MappingUtil.resolveMappingProperty(getter3);
		CasserPropertyNode p4 = MappingUtil.resolveMappingProperty(getter4);
		CasserPropertyNode p5 = MappingUtil.resolveMappingProperty(getter5);
		CasserPropertyNode p6 = MappingUtil.resolveMappingProperty(getter6);
		CasserPropertyNode p7 = MappingUtil.resolveMappingProperty(getter7);
		return new SelectOperation<Tuple7<V1, V2, V3, V4, V5, V6, V7>>(this, 
				new Tuple7.Mapper<V1, V2, V3, V4, V5, V6, V7>(
				getValueProvider(), 
				p1, p2, p3, p4, p5, p6, p7), 
				p1, p2, p3, p4, p5, p6, p7);
	}
	
	public CountOperation count(Object dsl) {
		Objects.requireNonNull(dsl, "dsl is empty");
		
		Class<?> iface = MappingUtil.getMappingInterface(dsl);
		
		CasserMappingEntity entity = mappingRepository.getEntity(iface);
		
		return new CountOperation(this, entity);
	}
	
	public <V> UpdateOperation update() {
		return new UpdateOperation(this);
	}
	
	public <V> UpdateOperation update(Getter<V> getter, V v) {
		Objects.requireNonNull(getter, "field is empty");
		Objects.requireNonNull(v, "value is empty");

		CasserPropertyNode p = MappingUtil.resolveMappingProperty(getter);
		
		return new UpdateOperation(this, p, v);
	}
	
	public InsertOperation insert() {
		return new InsertOperation(this, true);
	}
	
	public InsertOperation insert(Object pojo) {
		Objects.requireNonNull(pojo, "pojo is empty");
		
		Class<?> iface = MappingUtil.getMappingInterface(pojo);
		
		CasserMappingEntity entity = mappingRepository.getEntity(iface);
		
		return new InsertOperation(this, entity, pojo, true);
	}
	
	public InsertOperation upsert() {
		return new InsertOperation(this, false);
	}
	
	public InsertOperation upsert(Object pojo) {
		Objects.requireNonNull(pojo, "pojo is empty");
		
		Class<?> iface = MappingUtil.getMappingInterface(pojo);
		
		CasserMappingEntity entity = mappingRepository.getEntity(iface);
		
		return new InsertOperation(this, entity, pojo, false);
	}
	
	public DeleteOperation delete(Object dsl) {
		Objects.requireNonNull(dsl, "dsl is empty");
		
		Class<?> iface = MappingUtil.getMappingInterface(dsl);
		
		CasserMappingEntity entity = mappingRepository.getEntity(iface);
		
		return new DeleteOperation(this, entity);
	}
	
	public Session getSession() {
		return session;
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
		
		mappingRepository.entities().forEach(e -> dropEntity(e));
		
	}
	
	private void dropEntity(CasserMappingEntity entity) {
				
		switch(entity.getType()) {
		
		case TABLE:
			execute(SchemaUtil.dropTable(entity));
			break;
			
		case USER_DEFINED_TYPE:
			execute(SchemaUtil.dropUserType(entity));
			break;
		
		}

		
	}
	
	

	
}
