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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import casser.mapping.CasserEntityType;
import casser.mapping.CasserMappingEntity;
import casser.mapping.CasserMappingRepository;
import casser.mapping.ColumnValuePreparer;
import casser.mapping.ColumnValueProvider;
import casser.mapping.RowColumnValueProvider;
import casser.mapping.StatementColumnValuePreparer;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.UserType;
import com.google.common.util.concurrent.MoreExecutors;


public class SessionInitializer extends AbstractSessionOperations {

	private final Session session;
	private String usingKeyspace;
	private boolean showCql = false;
	private Executor executor = MoreExecutors.sameThreadExecutor();
	
	private CasserMappingRepository mappingRepository = new CasserMappingRepository();
	
	private boolean dropRemovedColumns = false;
	
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
		return new RowColumnValueProvider(mappingRepository);
	}
	
	@Override
	public ColumnValuePreparer getValuePreparer() {
		return new StatementColumnValuePreparer(mappingRepository);
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
	
	public CasserSession get() {
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

		TableOperations tableOps = new TableOperations(this, dropRemovedColumns);
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
		String tableName = entity.getName();
		return getKeyspaceMetadata().getTable(tableName.toLowerCase());
		
	}

	private UserType getUserType(CasserMappingEntity entity) {
		String userTypeName = entity.getName();
		return getKeyspaceMetadata().getUserType(userTypeName.toLowerCase());
	}
}
