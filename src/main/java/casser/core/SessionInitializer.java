package casser.core;

import com.datastax.driver.core.Session;


public class SessionInitializer {

	private final Session session;
	
	public SessionInitializer(Session session) {
		this.session = session;
	}
	
	public SessionInitializer validate(Object... dsls) {
		return this;
	}

	public SessionInitializer update(Object... dsls) {
		return this;
	}

	public SessionInitializer create(Object... dsls) {
		return this;
	}

	public SessionInitializer createDrop(Object... dsls) {
		return this;
	}

	public SessionInitializer use(String keyspace) {
		session.execute("USE " + keyspace);
		return this;
	}
	
	public CasserSession get() {
		return new CasserSession(session);
	}

}
