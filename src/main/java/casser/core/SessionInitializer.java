package casser.core;

import java.util.HashSet;
import java.util.Set;

import casser.mapping.CasserMappingEntity;
import casser.support.CasserException;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;


public class SessionInitializer extends AbstractSessionOperations {

	private final Session session;
	private boolean showCql = false;
	private Set<CasserMappingEntity<?>> dropEntitiesOnClose = null;
	
	private boolean dropRemovedColumns = false;
	
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
	
	public SessionInitializer dropRemovedColumns(boolean enabled) {
		this.dropRemovedColumns = enabled;
		return this;
	}
	
	@Override
	boolean isShowCql() {
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
		session.execute("USE " + keyspace);
		return this;
	}
	
	public CasserSession get() {
		return new CasserSession(session, showCql, dropEntitiesOnClose);
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
		
		if (type == AutoDdl.CREATE || type == AutoDdl.CREATE_DROP) {
			createNewTable(entity);
		}
		else {
			TableMetadata tmd = getTableMetadata(entity);
			
			if (type == AutoDdl.VALIDATE) {
				
				if (tmd == null) {
					throw new CasserException("table not exists " + entity.getTableName() + "for entity " + entity.getEntityInterface());
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
		
		doExecute(cql);
		
	}
	
	private void validateTable(TableMetadata tmd, CasserMappingEntity<?> entity) {
		
		String cql = SchemaUtil.alterTableCql(tmd, entity, dropRemovedColumns);
		
		if (cql != null) {
			throw new CasserException("schema changed for entity " + entity.getEntityInterface() + ", apply this command: " + cql);
		}
	}
	
	private void alterTable(TableMetadata tmd, CasserMappingEntity<?> entity) {
		
		String cql = SchemaUtil.alterTableCql(tmd, entity, dropRemovedColumns);
		
		if (cql != null) {
			doExecute(cql);
		}
	}
}