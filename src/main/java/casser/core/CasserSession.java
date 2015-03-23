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
import java.util.Set;

import casser.core.dsl.Getter;
import casser.core.dsl.Setter;
import casser.core.operation.DeleteOperation;
import casser.core.operation.SelectOperation;
import casser.core.operation.UpdateOperation;
import casser.core.operation.UpsertOperation;
import casser.core.tuple.Tuple1;
import casser.core.tuple.Tuple2;
import casser.core.tuple.Tuple3;
import casser.mapping.CasserMappingEntity;
import casser.mapping.CasserMappingProperty;
import casser.mapping.MappingUtil;

import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.Session;

public class CasserSession extends AbstractSessionOperations implements Closeable {

	private final Session session;
	private final boolean showCql;
	private final Set<CasserMappingEntity<?>> dropEntitiesOnClose;
	private final CasserEntityCache entityCache;
	
	CasserSession(Session session, boolean showCql, Set<CasserMappingEntity<?>> dropEntitiesOnClose, CasserEntityCache entityCache) {
		this.session = session;
		this.showCql = showCql;
		this.dropEntitiesOnClose = dropEntitiesOnClose;
		this.entityCache = entityCache;
	}
	
	@Override
	public Session currentSession() {
		return session;
	}
	
	@Override
	public boolean isShowCql() {
		return showCql;
	}
	
	public <V1> SelectOperation<Tuple1<V1>> select(Getter<V1> getter1) {
		
		CasserMappingProperty<?> p1 = MappingUtil.resolveMappingProperty(getter1);
	
		return new SelectOperation<Tuple1<V1>>(this, new Tuple1.Mapper<V1>(p1), p1);
	}

	public <V1, V2> SelectOperation<Tuple2<V1, V2>> select(Getter<V1> getter1, Getter<V2> getter2) {
		return null;
	}

	public <V1, V2, V3> SelectOperation<Tuple3<V1, V2, V3>> select(Getter<V1> getter1, Getter<V2> getter2, Getter<V3> getter3) {
		return null;
	}

	public <V1> UpdateOperation update(Setter<V1> setter1, V1 v1) {
		return null;
	}
	
	public UpsertOperation upsert(Object pojo) {
		
		Class<?> iface = MappingUtil.getMappingInterface(pojo);
		
		CasserMappingEntity<?> entity = entityCache.getEntity(iface);
		
		return new UpsertOperation(this, entity, pojo);
	}
	
	public DeleteOperation delete(Object dsl) {
		
		Class<?> iface = MappingUtil.getMappingInterface(dsl);
		
		CasserMappingEntity<?> entity = entityCache.getEntity(iface);
		
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
