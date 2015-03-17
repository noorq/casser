package casser.core;

import java.lang.reflect.Proxy;

import casser.config.CasserSettings;
import casser.config.DefaultCasserSettings;
import casser.core.reflect.DslInvocationHandler;
import casser.core.reflect.PojoInvocationHandler;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;



public final class Casser {
	
	private static volatile CasserSettings casserSettings = new DefaultCasserSettings();
	
	private Casser() {	
	}
	
	public static CasserSettings settings() {
		return casserSettings;
	}

	public static CasserSettings configure(CasserSettings overrideSettings) {
		CasserSettings old = casserSettings;
		casserSettings = overrideSettings;
		return old;
	}

	public static SessionInitializer connect(Cluster cluster) {
		Session session = cluster.connect();
		return new SessionInitializer(session);
	}

	public static SessionInitializer connect(Cluster cluster, String keyspace) {
		Session session = cluster.connect(keyspace);
		return new SessionInitializer(session);
	}
	
	public static SessionInitializer init(Session session) {
		
		if (session == null) {
			throw new IllegalArgumentException("empty session");
		}
		
		return new SessionInitializer(session);
	}
	
	public static <E> E dsl(Class<E> iface) {
		return dsl(iface, iface.getClassLoader());
	}

	@SuppressWarnings("unchecked")
	public static <E> E dsl(Class<E> iface, ClassLoader classLoader) {
		DslInvocationHandler<E> handler = new DslInvocationHandler<E>(iface);
		E proxy = (E) Proxy.newProxyInstance(
		                            classLoader,
		                            new Class[] { iface },
		                            handler);
		return proxy;
	}

	public static <E> E pojo(Class<E> iface) {
		return pojo(iface, iface.getClassLoader());
	}

	@SuppressWarnings("unchecked")
	public static <E> E pojo(Class<E> iface, ClassLoader classLoader) {
		PojoInvocationHandler<E> handler = new PojoInvocationHandler<E>(iface);
		E proxy = (E) Proxy.newProxyInstance(
		                            classLoader,
		                            new Class[] { iface },
		                            handler);
		return proxy;
	}

}
