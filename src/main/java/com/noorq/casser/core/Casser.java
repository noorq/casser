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
package com.noorq.casser.core;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.noorq.casser.config.CasserSettings;
import com.noorq.casser.config.DefaultCasserSettings;
import com.noorq.casser.core.reflect.CasserPropertyNode;
import com.noorq.casser.core.reflect.DslExportable;
import com.noorq.casser.mapping.CasserEntity;
import com.noorq.casser.support.CasserMappingException;



public final class Casser {
	
	private static volatile CasserSettings settings = new DefaultCasserSettings();
	
	private static final ConcurrentMap<Class<?>, Object> dslCache = new  ConcurrentHashMap<Class<?>, Object>();
	
	private static volatile CasserSession session;
	
	private Casser() {
	}
	
	public static CasserSession session() {
		return Objects.requireNonNull(session, "session is not initialized");
	}
	
	protected static synchronized void singelton(CasserSession newSession) {
		if (session == null) {
			session = newSession;
		}
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
	
	public static void clearDslCache() {
		dslCache.clear();
	}
	
	public static <E> E dsl(Class<E> iface) {
		return dsl(iface, iface.getClassLoader(), Optional.empty());
	}

	public static <E> E dsl(Class<E> iface, ClassLoader classLoader) {
		return dsl(iface, classLoader, Optional.empty());
	}
	
	public static <E> E dsl(Class<E> iface, ClassLoader classLoader, Optional<CasserPropertyNode> parent) {
		
		Object instance = null;
		
		if (!parent.isPresent()) {
			 instance = dslCache.get(iface);
		}
		
		if (instance == null) {
		
			instance = settings.getDslInstantiator().instantiate(iface, classLoader, parent);
			
			if (!parent.isPresent()) {
				
				Object c = dslCache.putIfAbsent(iface, instance);
				if (c != null) {
					instance = c;
				}
				
			}
		}
		
		return (E) instance;
	}
	
	public static <E> E map(Class<E> iface, Map<String, Object> src) {
		return map(iface, src, iface.getClassLoader());
	}

	public static <E> E map(Class<E> iface, Map<String, Object> src, ClassLoader classLoader) {
		return settings.getMapperInstantiator().instantiate(iface, src, classLoader);
	}

	public static CasserEntity entity(Class<?> iface) {
		
		Object dsl = dsl(iface);
		
		DslExportable e = (DslExportable) dsl;
		
		return e.getCasserMappingEntity();
	}
	
	public static CasserEntity resolve(Object ifaceOrDsl) {
		
		if (ifaceOrDsl == null) {
			throw new CasserMappingException("ifaceOrDsl is null");
		}
		
		if (ifaceOrDsl instanceof DslExportable) {
			
			DslExportable e = (DslExportable) ifaceOrDsl;
			
			return e.getCasserMappingEntity();
		}
		
		if (ifaceOrDsl instanceof Class) {
			
			Class<?> iface = (Class<?>) ifaceOrDsl;

			if (!iface.isInterface()) {
				throw new CasserMappingException("class is not an interface " + iface);
			}
			
			return entity(iface);

		}
		
		throw new CasserMappingException("unknown dsl object or mapping interface " + ifaceOrDsl);
	}
	
}
