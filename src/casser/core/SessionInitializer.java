package casser.core;


public class SessionInitializer {

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
		return this;
	}
	
	public Session get() {
		return null;
	}

	
	
}
