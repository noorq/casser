package casser.core;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;



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
	
	public static SessionInitializer connect(String host) {
		return null;
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
		InvocationHandler handler = new PojoInvocationHandler();
		E proxy = (E) Proxy.newProxyInstance(
		                            classLoader,
		                            new Class[] { iface },
		                            handler);
		return proxy;
	}

}
