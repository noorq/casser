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

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import casser.mapping.CasserMappingEntity;
import casser.mapping.CasserMappingRepository;
import casser.mapping.MappingUtil;
import casser.support.CasserException;
import casser.support.Requires;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.google.common.util.concurrent.MoreExecutors;


public class SessionInitializer extends AbstractSessionOperations {

	private final Session session;
	private String usingKeyspace;
	private boolean showCql = false;
	private Executor executor = MoreExecutors.sameThreadExecutor();
	private Set<CasserMappingEntity<?>> dropEntitiesOnClose = null;
	
	private CasserMappingRepository mappingRepository = new CasserMappingRepository();
	
	private boolean dropRemovedColumns = false;
	
	private KeyspaceMetadata keyspaceMetadata;
	
	SessionInitializer(Session session) {
		this.session = Objects.requireNonNull(session, "empty session");
		this.usingKeyspace = session.getLoggedKeyspace(); // can be null
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
	public Executor getExecutor() {
		return executor;
	}

	public SessionInitializer showCql() {
		this.showCql = true;
		return this;
	}
	
	public SessionInitializer showCql(boolean enabled) {
		this.showCql = enabled;
		return this;
	}
	
	public SessionInitializer withExecutor(Executor executor) {
		Objects.requireNonNull(executor, "empty executor");
		this.executor = executor;
		return this;
	}

	public SessionInitializer withCachingExecutor() {
		this.executor = Executors.newCachedThreadPool();
		return this;
	}

	public SessionInitializer dropRemovedColumns(boolean enabled) {
		this.dropRemovedColumns = enabled;
		return this;
	}
	
	@Override
	public boolean isShowCql() {
		return showCql;
	}
	
	public SessionInitializer validate(Object... dsls) {
		initialize(AutoDsl.VALIDATE, dsls);
		return this;
	}

	public SessionInitializer update(Object... dsls) {
		initialize(AutoDsl.UPDATE, dsls);
		return this;
	}

	public SessionInitializer create(Object... dsls) {
		initialize(AutoDsl.CREATE, dsls);
		return this;
	}

	public SessionInitializer createDrop(Object... dsls) {
		initialize(AutoDsl.CREATE_DROP, dsls);
		return this;
	}

	public SessionInitializer use(String keyspace) {
		session.execute(SchemaUtil.useCql(keyspace, false));
		this.usingKeyspace = keyspace;
		return this;
	}
	
	public SessionInitializer use(String keyspace, boolean forceQuote) {
		session.execute(SchemaUtil.useCql(keyspace, forceQuote));
		this.usingKeyspace = keyspace;
		return this;
	}
	
	public CasserSession get() {
		return new CasserSession(session, 
				usingKeyspace,
				showCql, 
				dropEntitiesOnClose, 
				mappingRepository,
				executor);
	}

	private enum AutoDsl {
		VALIDATE,
		UPDATE,
		CREATE,
		CREATE_DROP;
	}
	
	private void initialize(AutoDsl type, Object[] dsls) {
		
		Objects.requireNonNull(usingKeyspace, "please define keyspace by 'use' operator");
		Requires.nonNullArray(dsls);
		
		KeyspaceMetadata keyspaceMetadata = getKeyspaceMetadata();
		
		mappingRepository.addEntities(dsls);

		for (CasserMappingEntity<?> entity : mappingRepository.getKnownEntities()) {
			initializeTable(type, entity);
		}
		
	}
	
	private void initializeTable(AutoDsl type, CasserMappingEntity<?> entity) {
		
		if (type == AutoDsl.CREATE || type == AutoDsl.CREATE_DROP) {
			createNewTable(entity);
		}
		else {
			TableMetadata tmd = getTableMetadata(entity);
			
			if (type == AutoDsl.VALIDATE) {
				
				if (tmd == null) {
					throw new CasserException("table not exists " + entity.getTableName() + "for entity " + entity.getMappingInterface());
				}
				
				validateTable(tmd, entity);
			}
			else if (type == AutoDsl.UPDATE) {
				
				if (tmd == null) {
					createNewTable(entity);
				}
				else {
					alterTable(tmd, entity);
				}
				
			}
		}
		
		if (type == AutoDsl.CREATE_DROP) {
			getOrCreateDropEntitiesSet().add(entity);
		}
		
	}
	
	private Set<CasserMappingEntity<?>> getOrCreateDropEntitiesSet() {
		if (dropEntitiesOnClose == null) {
			dropEntitiesOnClose = new HashSet<CasserMappingEntity<?>>();
		}
		return dropEntitiesOnClose;
	}
	
	private KeyspaceMetadata getKeyspaceMetadata() {
		if (keyspaceMetadata == null) {
			keyspaceMetadata = session.getCluster().getMetadata().getKeyspace(usingKeyspace.toLowerCase());
		}
		return keyspaceMetadata;
	}
	
	private TableMetadata getTableMetadata(CasserMappingEntity<?> entity) {
		String tableName = entity.getTableName();
		return getKeyspaceMetadata().getTable(tableName.toLowerCase());
		
	}
	
	private void createNewTable(CasserMappingEntity<?> entity) {
		
		String cql = SchemaUtil.createTableCql(entity);
		
		execute(cql);
		
	}
	
	private void validateTable(TableMetadata tmd, CasserMappingEntity<?> entity) {
		
		String cql = SchemaUtil.alterTableCql(tmd, entity, dropRemovedColumns);
		
		if (cql != null) {
			throw new CasserException("schema changed for entity " + entity.getMappingInterface() + ", apply this command: " + cql);
		}
	}
	
	private void alterTable(TableMetadata tmd, CasserMappingEntity<?> entity) {
		
		String cql = SchemaUtil.alterTableCql(tmd, entity, dropRemovedColumns);
		
		if (cql != null) {
			execute(cql);
		}
	}
}
