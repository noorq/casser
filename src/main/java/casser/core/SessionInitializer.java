package casser.core;

import java.util.HashSet;
import java.util.Set;

import casser.mapping.CasserMappingEntity;
import casser.mapping.MappingUtil;
import casser.support.CasserException;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;


public class SessionInitializer extends AbstractSessionOperations {

	private final Session session;
	private boolean showCql = false;
	private Set<CasserMappingEntity<?>> dropEntitiesOnClose = null;
	
	private MappingEntityFactory entityFactory = new MappingEntityFactory();
	
	private boolean dropRemovedColumns = false;
	
	SessionInitializer(Session session) {
		
		if (session == null) {
			throw new IllegalArgumentException("empty session");
		}
		
		this.session = session;
	}
	
	@Override
	public Session currentSession() {
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
	
	public SessionInitializer dropRemovedColumns(boolean enabled) {
		this.dropRemovedColumns = enabled;
		return this;
	}
	
	@Override
	public boolean isShowCql() {
		return showCql;
	}
	
	public SessionInitializer validate(Object... dsls) {
		process(AutoDdl.VALIDATE, dsls);
		return this;
	}

	public SessionInitializer update(Object... dsls) {
		process(AutoDdl.UPDATE, dsls);
		return this;
	}

	public SessionInitializer create(Object... dsls) {
		process(AutoDdl.CREATE, dsls);
		return this;
	}

	public SessionInitializer createDrop(Object... dsls) {
		process(AutoDdl.CREATE_DROP, dsls);
		return this;
	}

	public SessionInitializer use(String keyspace) {
		session.execute(SchemaUtil.useCql(keyspace));
		return this;
	}
	
	public CasserSession get() {
		return new CasserSession(session, showCql, dropEntitiesOnClose, entityFactory);
	}

	private enum AutoDdl {
		VALIDATE,
		UPDATE,
		CREATE,
		CREATE_DROP;
	}
	
	private void process(AutoDdl type, Object[] dsls) {
		
		for (Object dsl : dsls) {
			processSingle(type, dsl);
		}
		
	}
	
	private void processSingle(AutoDdl type, Object dsl) {
		
		Class<?> iface = MappingUtil.getMappingInterface(dsl);
		
		CasserMappingEntity<?> entity = entityFactory.getEntity(iface);
		
		if (type == AutoDdl.CREATE || type == AutoDdl.CREATE_DROP) {
			createNewTable(entity);
		}
		else {
			TableMetadata tmd = getTableMetadata(entity);
			
			if (type == AutoDdl.VALIDATE) {
				
				if (tmd == null) {
					throw new CasserException("table not exists " + entity.getTableName() + "for entity " + entity.getMappingInterface());
				}
				
				validateTable(tmd, entity);
			}
			else if (type == AutoDdl.UPDATE) {
				
				if (tmd == null) {
					createNewTable(entity);
				}
				else {
					alterTable(tmd, entity);
				}
				
			}
		}
		
		if (type == AutoDdl.CREATE_DROP) {
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
