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

import java.lang.reflect.Proxy;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import casser.config.CasserSettings;
import casser.config.DefaultCasserSettings;
import casser.core.reflect.DslInvocationHandler;
import casser.core.reflect.PojoInvocationHandler;
import casser.mapping.convert.UDTValueWritable;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;



public final class Casser {
	
	private static volatile CasserSettings settings = new DefaultCasserSettings();
	
	private static final ConcurrentMap<Class<?>, Object> dslCache = new  ConcurrentHashMap<Class<?>, Object>();
	
	private Casser() {
	}
	
	public static CasserSettings settings() {
		return settings;
	}

	public static CasserSettings configure(CasserSettings overrideSettings) {
		CasserSettings old = settings;
		settings = overrideSettings;
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

	public static <E> E dsl(Class<E> iface, ClassLoader classLoader) {
		
		Object instance = dslCache.get(iface);
		
		if (instance == null) {
		
			instance = settings.getDslInstantiator().instantiate(iface, classLoader);
			
			Object c = dslCache.putIfAbsent(iface, instance);
			if (c != null) {
				instance = c;
			}
		}
		
		return (E) instance;
		
	}

	public static <E> E pojo(Class<E> iface) {
		return pojo(iface, iface.getClassLoader());
	}

	public static <E> E pojo(Class<E> iface, ClassLoader classLoader) {
		return settings.getPojoInstantiator().instantiate(iface, classLoader);
	}

}
