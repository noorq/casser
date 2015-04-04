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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.UserType;
import com.google.common.util.concurrent.MoreExecutors;
import com.noorq.casser.mapping.CasserEntityType;
import com.noorq.casser.mapping.CasserMappingEntity;
import com.noorq.casser.mapping.MappingRepositoryBuilder;
import com.noorq.casser.mapping.value.ColumnValuePreparer;
import com.noorq.casser.mapping.value.ColumnValueProvider;
import com.noorq.casser.support.CasserException;


public class SessionInitializer extends AbstractSessionOperations {

	private final Session session;
	private String usingKeyspace;
	private boolean showCql = false;
	private Executor executor = MoreExecutors.sameThreadExecutor();
	
	private MappingRepositoryBuilder mappingRepository = new MappingRepositoryBuilder();
	
	private boolean dropUnusedColumns = false;
	private boolean dropUnusedIndexes = false;
	
	private KeyspaceMetadata keyspaceMetadata;
	
	private final List<Object> initList = new ArrayList<Object>();
	private AutoDsl autoDsl = AutoDsl.UPDATE;
	
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
	
	@Override
	public ColumnValueProvider getValueProvider() {
		throw new CasserException("not expected call");
	}
	
	@Override
	public ColumnValuePreparer getValuePreparer() {
		throw new CasserException("not expected call");
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

	public SessionInitializer dropUnusedColumns(boolean enabled) {
		this.dropUnusedColumns = enabled;
		return this;
	}

	public SessionInitializer dropUnusedIndexes(boolean enabled) {
		this.dropUnusedIndexes = enabled;
		return this;
	}

	@Override
	public boolean isShowCql() {
		return showCql;
	}
	
	public SessionInitializer add(Object... dsls) {
		Objects.requireNonNull(dsls, "dsls is empty");
		int len = dsls.length;
		for (int i = 0; i != len; ++i) {
			Object obj = Objects.requireNonNull(dsls[i], "element " + i + " is empty");
			initList.add(obj);
		}
		return this;
	}
	
	public SessionInitializer autoValidate() {
		this.autoDsl = AutoDsl.VALIDATE;
		return this;
	}

	public SessionInitializer autoUpdate() {
		this.autoDsl = AutoDsl.UPDATE;
		return this;
	}

	public SessionInitializer autoCreate() {
		this.autoDsl = AutoDsl.CREATE;
		return this;
	}

	public SessionInitializer autoCreateDrop() {
		this.autoDsl = AutoDsl.CREATE_DROP;
		return this;
	}

	public SessionInitializer auto(AutoDsl autoDsl) {
		this.autoDsl = autoDsl;
		return this;
	}
	
	public SessionInitializer use(String keyspace) {
		session.execute(SchemaUtil.use(keyspace, false));
		this.usingKeyspace = keyspace;
		return this;
	}
	
	public SessionInitializer use(String keyspace, boolean forceQuote) {
		session.execute(SchemaUtil.use(keyspace, forceQuote));
		this.usingKeyspace = keyspace;
		return this;
	}
	
	public synchronized CasserSession get() {
		initialize();
		return new CasserSession(session, 
				usingKeyspace,
				showCql, 
				mappingRepository,
				executor,
				autoDsl == AutoDsl.CREATE_DROP);
	}

	private void initialize() {
		
		Objects.requireNonNull(usingKeyspace, "please define keyspace by 'use' operator");

		initList.forEach(dsl -> mappingRepository.add(dsl));

		TableOperations tableOps = new TableOperations(this, dropUnusedColumns, dropUnusedIndexes);
		UserTypeOperations userTypeOps = new UserTypeOperations(this);
		
		switch(autoDsl) {
		
		case CREATE:
		case CREATE_DROP:
			
			mappingRepository.entities().stream().filter(e -> e.getType() == CasserEntityType.USER_DEFINED_TYPE)
				.forEach(e -> userTypeOps.createUserType(e));
			
			mappingRepository.entities().stream().filter(e -> e.getType() == CasserEntityType.TABLE)
				.forEach(e -> tableOps.createTable(e));
			
			break;
			
		case VALIDATE:
			mappingRepository.entities().stream().filter(e -> e.getType() == CasserEntityType.USER_DEFINED_TYPE)
				.forEach(e -> userTypeOps.validateUserType(getUserType(e), e));
			
			mappingRepository.entities().stream().filter(e -> e.getType() == CasserEntityType.TABLE)
				.forEach(e -> tableOps.validateTable(getTableMetadata(e), e));
			break;
			
		case UPDATE:
			mappingRepository.entities().stream().filter(e -> e.getType() == CasserEntityType.USER_DEFINED_TYPE)
				.forEach(e -> userTypeOps.updateUserType(getUserType(e), e));

			mappingRepository.entities().stream().filter(e -> e.getType() == CasserEntityType.TABLE)
				.forEach(e -> tableOps.updateTable(getTableMetadata(e), e));
			break;
		
		}
		
		KeyspaceMetadata km = getKeyspaceMetadata();
		
		for (UserType userType : km.getUserTypes()) {
			mappingRepository.addUserType(userType.getTypeName(), userType);
		}
		
	}
	
	private KeyspaceMetadata getKeyspaceMetadata() {
		if (keyspaceMetadata == null) {
			keyspaceMetadata = session.getCluster().getMetadata().getKeyspace(usingKeyspace.toLowerCase());
		}
		return keyspaceMetadata;
	}
	
	private TableMetadata getTableMetadata(CasserMappingEntity entity) {
		return getKeyspaceMetadata().getTable(entity.getName().getName());
		
	}

	private UserType getUserType(CasserMappingEntity entity) {
		return getKeyspaceMetadata().getUserType(entity.getName().getName());
	}
}
