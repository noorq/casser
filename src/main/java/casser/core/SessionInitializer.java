package casser.core;

import casser.mapping.CasserMappingEntity;
import casser.support.CasserException;

import com.datastax.driver.core.Session;


public class SessionInitializer {

	private final Session session;
	
	public SessionInitializer(Session session) {
		this.session = session;
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
		return new CasserSession(session);
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
		
		
		
	}
}
