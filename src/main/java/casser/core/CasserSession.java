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
package casser.core;

import java.io.Closeable;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;

import casser.core.dsl.Getter;
import casser.core.dsl.Setter;
import casser.core.operation.CountOperation;
import casser.core.operation.DeleteOperation;
import casser.core.operation.SelectOperation;
import casser.core.operation.UpdateOperation;
import casser.core.operation.UpsertOperation;
import casser.core.tuple.Tuple1;
import casser.core.tuple.Tuple2;
import casser.core.tuple.Tuple3;
import casser.core.tuple.Tuple4;
import casser.core.tuple.Tuple5;
import casser.core.tuple.Tuple6;
import casser.core.tuple.Tuple7;
import casser.mapping.CasserMappingEntity;
import casser.mapping.CasserMappingProperty;
import casser.mapping.MappingUtil;

import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.Session;

public class CasserSession extends AbstractSessionOperations implements Closeable {

	private final Session session;
	private volatile boolean showCql;
	private final Set<CasserMappingEntity<?>> dropEntitiesOnClose;
	private final CasserEntityCache entityCache;
	private final Executor executor;
	
	CasserSession(Session session, 
			boolean showCql, 
			Set<CasserMappingEntity<?>> dropEntitiesOnClose, 
			CasserEntityCache entityCache, 
			Executor executor) {
		this.session = session;
		this.showCql = showCql;
		this.dropEntitiesOnClose = dropEntitiesOnClose;
		this.entityCache = entityCache;
		this.executor = executor;
	}
	
	@Override
	public Session currentSession() {
		return session;
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

	public <V1> SelectOperation<Tuple1<V1>> select(Getter<V1> getter1) {
		Objects.requireNonNull(getter1, "field 1 is empty");
		
		CasserMappingProperty<?> p1 = MappingUtil.resolveMappingProperty(getter1);
		return new SelectOperation<Tuple1<V1>>(this, new Tuple1.Mapper<V1>(p1), p1);
	}

	public <V1, V2> SelectOperation<Tuple2<V1, V2>> select(Getter<V1> getter1, Getter<V2> getter2) {
		Objects.requireNonNull(getter1, "field 1 is empty");
		Objects.requireNonNull(getter2, "field 2 is empty");
		
		CasserMappingProperty<?> p1 = MappingUtil.resolveMappingProperty(getter1);
		CasserMappingProperty<?> p2 = MappingUtil.resolveMappingProperty(getter2);
		return new SelectOperation<Tuple2<V1, V2>>(this, new Tuple2.Mapper<V1, V2>(p1, p2), p1, p2);
	}

	public <V1, V2, V3> SelectOperation<Tuple3<V1, V2, V3>> select(Getter<V1> getter1, Getter<V2> getter2, Getter<V3> getter3) {
		Objects.requireNonNull(getter1, "field 1 is empty");
		Objects.requireNonNull(getter2, "field 2 is empty");
		Objects.requireNonNull(getter3, "field 3 is empty");
		
		CasserMappingProperty<?> p1 = MappingUtil.resolveMappingProperty(getter1);
		CasserMappingProperty<?> p2 = MappingUtil.resolveMappingProperty(getter2);
		CasserMappingProperty<?> p3 = MappingUtil.resolveMappingProperty(getter3);
		return new SelectOperation<Tuple3<V1, V2, V3>>(this, new Tuple3.Mapper<V1, V2, V3>(p1, p2, p3), p1, p2, p3);
	}

	public <V1, V2, V3, V4> SelectOperation<Tuple4<V1, V2, V3, V4>> select(
			Getter<V1> getter1, Getter<V2> getter2, Getter<V3> getter3, Getter<V4> getter4) {
		Objects.requireNonNull(getter1, "field 1 is empty");
		Objects.requireNonNull(getter2, "field 2 is empty");
		Objects.requireNonNull(getter3, "field 3 is empty");
		Objects.requireNonNull(getter4, "field 4 is empty");
		
		CasserMappingProperty<?> p1 = MappingUtil.resolveMappingProperty(getter1);
		CasserMappingProperty<?> p2 = MappingUtil.resolveMappingProperty(getter2);
		CasserMappingProperty<?> p3 = MappingUtil.resolveMappingProperty(getter3);
		CasserMappingProperty<?> p4 = MappingUtil.resolveMappingProperty(getter4);
		return new SelectOperation<Tuple4<V1, V2, V3, V4>>(this, new Tuple4.Mapper<V1, V2, V3, V4>(p1, p2, p3, p4), p1, p2, p3, p4);
	}

	public <V1, V2, V3, V4, V5> SelectOperation<Tuple5<V1, V2, V3, V4, V5>> select(
			Getter<V1> getter1, Getter<V2> getter2, Getter<V3> getter3, Getter<V4> getter4, Getter<V5> getter5) {
		Objects.requireNonNull(getter1, "field 1 is empty");
		Objects.requireNonNull(getter2, "field 2 is empty");
		Objects.requireNonNull(getter3, "field 3 is empty");
		Objects.requireNonNull(getter4, "field 4 is empty");
		Objects.requireNonNull(getter5, "field 5 is empty");
		
		CasserMappingProperty<?> p1 = MappingUtil.resolveMappingProperty(getter1);
		CasserMappingProperty<?> p2 = MappingUtil.resolveMappingProperty(getter2);
		CasserMappingProperty<?> p3 = MappingUtil.resolveMappingProperty(getter3);
		CasserMappingProperty<?> p4 = MappingUtil.resolveMappingProperty(getter4);
		CasserMappingProperty<?> p5 = MappingUtil.resolveMappingProperty(getter5);
		return new SelectOperation<Tuple5<V1, V2, V3, V4, V5>>(this, 
				new Tuple5.Mapper<V1, V2, V3, V4, V5>(p1, p2, p3, p4, p5), 
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
		
		CasserMappingProperty<?> p1 = MappingUtil.resolveMappingProperty(getter1);
		CasserMappingProperty<?> p2 = MappingUtil.resolveMappingProperty(getter2);
		CasserMappingProperty<?> p3 = MappingUtil.resolveMappingProperty(getter3);
		CasserMappingProperty<?> p4 = MappingUtil.resolveMappingProperty(getter4);
		CasserMappingProperty<?> p5 = MappingUtil.resolveMappingProperty(getter5);
		CasserMappingProperty<?> p6 = MappingUtil.resolveMappingProperty(getter6);
		return new SelectOperation<Tuple6<V1, V2, V3, V4, V5, V6>>(this, 
				new Tuple6.Mapper<V1, V2, V3, V4, V5, V6>(p1, p2, p3, p4, p5, p6), 
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
		
		CasserMappingProperty<?> p1 = MappingUtil.resolveMappingProperty(getter1);
		CasserMappingProperty<?> p2 = MappingUtil.resolveMappingProperty(getter2);
		CasserMappingProperty<?> p3 = MappingUtil.resolveMappingProperty(getter3);
		CasserMappingProperty<?> p4 = MappingUtil.resolveMappingProperty(getter4);
		CasserMappingProperty<?> p5 = MappingUtil.resolveMappingProperty(getter5);
		CasserMappingProperty<?> p6 = MappingUtil.resolveMappingProperty(getter6);
		CasserMappingProperty<?> p7 = MappingUtil.resolveMappingProperty(getter7);
		return new SelectOperation<Tuple7<V1, V2, V3, V4, V5, V6, V7>>(this, 
				new Tuple7.Mapper<V1, V2, V3, V4, V5, V6, V7>(p1, p2, p3, p4, p5, p6, p7), 
				p1, p2, p3, p4, p5, p6, p7);
	}
	
	public CountOperation count(Object dsl) {
		Objects.requireNonNull(dsl, "dsl is empty");
		
		Class<?> iface = MappingUtil.getMappingInterface(dsl);
		
		CasserMappingEntity<?> entity = entityCache.getOrCreateEntity(iface);
		
		return new CountOperation(this, entity);
	}
	
	public <V> UpdateOperation update(Setter<V> setter, V v) {
		Objects.requireNonNull(setter, "field is empty");
		Objects.requireNonNull(v, "value is empty");

		CasserMappingProperty<?> p = MappingUtil.resolveMappingProperty(setter);
		
		return new UpdateOperation(this, p, v);
	}
	
	public UpsertOperation upsert(Object pojo) {
		Objects.requireNonNull(pojo, "pojo is empty");
		
		Class<?> iface = MappingUtil.getMappingInterface(pojo);
		
		CasserMappingEntity<?> entity = entityCache.getOrCreateEntity(iface);
		
		return new UpsertOperation(this, entity, pojo);
	}
	
	public DeleteOperation delete(Object dsl) {
		Objects.requireNonNull(dsl, "dsl is empty");
		
		Class<?> iface = MappingUtil.getMappingInterface(dsl);
		
		CasserMappingEntity<?> entity = entityCache.getOrCreateEntity(iface);
		
		return new DeleteOperation(this, entity);
	}
	
	public Session getSession() {
		return session;
	}

	public void close() {
		dropEntitiesIfNeeded();
		session.close();
	}
	
	public CloseFuture closeAsync() {
		dropEntitiesIfNeeded();
		return session.closeAsync();
	}
	
	private void dropEntitiesIfNeeded() {
		
		if (dropEntitiesOnClose == null || dropEntitiesOnClose.isEmpty()) {
			return;
		}
		
		for (CasserMappingEntity<?> entity : dropEntitiesOnClose) {
			dropEntity(entity);
		}
		
	}
	
	private void dropEntity(CasserMappingEntity<?> entity) {
		
		String cql = SchemaUtil.dropTableCql(entity);
		
		execute(cql);
		
	}
	
	

	
}
