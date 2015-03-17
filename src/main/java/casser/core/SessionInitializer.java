package casser.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import casser.mapping.CasserMappingEntity;
import casser.mapping.CasserMappingProperty;
import casser.support.CasserException;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;


public class SessionInitializer extends AbstractSessionOperations {

	private final Session session;
	private boolean showCql = false;
	private Set<CasserMappingEntity<?>> dropEntitiesOnClose = null;
	
	SessionInitializer(Session session) {
		
		if (session == null) {
			throw new IllegalArgumentException("empty session");
		}
		
		this.session = session;
	}
	
	@Override
	Session currentSession() {
		return session;
	}

	public SessionInitializer showCql() {
		this.showCql = true;
		return this;
	}
	
	public SessionInitializer showCql(boolean enabled) {
		this.showCql = enabled;
		return this;
	}
	
	@Override
	boolean isShowCql() {
		return showCql;
	}
	
	public SessionInitializer validate(Object... dsls) {
		process(AutoDslType.VALIDATE, dsls);
		return this;
	}

	public SessionInitializer update(Object... dsls) {
		process(AutoDslType.UPDATE, dsls);
		return this;
	}

	public SessionInitializer create(Object... dsls) {
		process(AutoDslType.CREATE, dsls);
		return this;
	}

	public SessionInitializer createDrop(Object... dsls) {
		process(AutoDslType.CREATE_DROP, dsls);
		return this;
	}

	public SessionInitializer use(String keyspace) {
		session.execute("USE " + keyspace);
		return this;
	}
	
	public CasserSession get() {
		return new CasserSession(session, showCql, dropEntitiesOnClose);
	}

	private enum AutoDslType {
		VALIDATE,
		UPDATE,
		CREATE,
		CREATE_DROP;
	}
	
	private void process(AutoDslType type, Object[] dsls) {
		
		for (Object dsl : dsls) {
			processSingle(type, dsl);
		}
		
	}
	
	private void processSingle(AutoDslType type, Object dsl) {
		
		Class<?> iface = null;
		
		if (dsl instanceof Class) {
			iface = (Class<?>) dsl;
			
			if (!iface.isInterface()) {
				throw new CasserException("expected interface " + iface);
			}
			
		}
		else {
			Class<?>[] ifaces = dsl.getClass().getInterfaces();
			if (ifaces.length != 1) {
				throw new CasserException("supports only single interface, wrong dsl class " + dsl.getClass()
						);
			}
			
			iface = ifaces[0];
		}
		
		
		CasserMappingEntity<?> entity = new CasserMappingEntity(iface);
		
		if (type == AutoDslType.CREATE || type == AutoDslType.CREATE_DROP) {
			createNewTable(entity);
		}
		else {
			TableMetadata tmd = getTableMetadata(entity);
			
			if (type == AutoDslType.VALIDATE) {
				
				if (tmd == null) {
					throw new CasserException("table not exists " + entity.getTableName() + "for entity " + entity.getEntityInterface());
				}
				
				validateTable(tmd, entity);
			}
			else if (type == AutoDslType.UPDATE) {
				
				if (tmd == null) {
					createNewTable(entity);
				}
				else {
					alterTable(tmd, entity);
				}
				
			}
		}
		
		if (type == AutoDslType.CREATE_DROP) {
			getOrCreateDropEntitiesSet().add(entity);
		}
		
	}
	
	private Set<CasserMappingEntity<?>> getOrCreateDropEntitiesSet() {
		if (dropEntitiesOnClose == null) {
			dropEntitiesOnClose = new HashSet<CasserMappingEntity<?>>();
		}
		return dropEntitiesOnClose;
	}
	
	private TableMetadata getTableMetadata(CasserMappingEntity<?> entity) {
		
		String tableName = entity.getTableName();
		
		return session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace().toLowerCase()).getTable(tableName.toLowerCase());
		
	}
	
	private void createNewTable(CasserMappingEntity<?> entity) {
		
		Create create = SchemaBuilder.createTable(entity.getTableName());
		
		List<CasserMappingProperty<?>> partitionKeys = new ArrayList<CasserMappingProperty<?>>();
		List<CasserMappingProperty<?>> clusteringColumns = new ArrayList<CasserMappingProperty<?>>();
		List<CasserMappingProperty<?>> columns = new ArrayList<CasserMappingProperty<?>>();
		
		for (CasserMappingProperty<?> prop : entity.getMappingProperties()) {
			
			if (prop.isPartitionKey()) {
				partitionKeys.add(prop);
			}
			else if (prop.isClusteringColumn()) {
				clusteringColumns.add(prop);
			}
			else {
				columns.add(prop);
			}
			
		}
		
		Collections.sort(partitionKeys, OrdinalBasedPropertyComparator.INSTANCE);
		Collections.sort(clusteringColumns, OrdinalBasedPropertyComparator.INSTANCE);
		
		for (CasserMappingProperty<?> prop : partitionKeys) {
			create.addPartitionKey(prop.getColumnName(), prop.getDataType());
		}
		
		for (CasserMappingProperty<?> prop : clusteringColumns) {
			create.addClusteringColumn(prop.getColumnName(), prop.getDataType());
		}
		
		for (CasserMappingProperty<?> prop : columns) {
			create.addColumn(prop.getColumnName(), prop.getDataType());
		}
		
		String cql = create.getQueryString();
		
		doExecute(cql);
		
	}
	
	private void validateTable(TableMetadata tmd, CasserMappingEntity<?> entity) {
		
	}
	
	private void alterTable(TableMetadata tmd, CasserMappingEntity<?> entity) {
		
	}
}
