/*
 *      Copyright (C) 2015 The Helenus Authors
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
package net.helenus.core;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

import net.helenus.config.DefaultHelenusSettings;
import net.helenus.config.HelenusSettings;
import net.helenus.core.reflect.DslExportable;
import net.helenus.core.reflect.HelenusPropertyNode;
import net.helenus.mapping.HelenusEntity;
import net.helenus.support.HelenusMappingException;

public final class Helenus {

	private static volatile HelenusSettings settings = new DefaultHelenusSettings();
	private static final ConcurrentMap<Class<?>, Object> dslCache = new ConcurrentHashMap<Class<?>, Object>();
	private static final ConcurrentMap<Class<?>, Metadata> metadataForEntity = new ConcurrentHashMap<Class<?>, Metadata>();
	private static final Set<HelenusSession> sessions = new HashSet<HelenusSession>();
	private static volatile HelenusSession singleton;

	private Helenus() {
	}

	protected static void setSession(HelenusSession session) {
		sessions.add(session);
		singleton = session;
	}

	public static HelenusSession session() {
		return singleton;
	}

	public static void shutdown() {
		sessions.forEach((session) -> {
			session.close();
			sessions.remove(session);
		});
		dslCache.clear();
	}

	public static HelenusSettings settings() {
		return settings;
	}

	public static HelenusSettings settings(HelenusSettings overrideSettings) {
		HelenusSettings old = settings;
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
		return dsl(iface, null);
	}

	public static <E> E dsl(Class<E> iface, Metadata metadata) {
		return dsl(iface, iface.getClassLoader(), Optional.empty(), metadata);
	}

	public static <E> E dsl(Class<E> iface, ClassLoader classLoader, Metadata metadata) {
		return dsl(iface, classLoader, Optional.empty(), metadata);
	}

	public static <E> E dsl(Class<E> iface, ClassLoader classLoader, Optional<HelenusPropertyNode> parent,
			Metadata metadata) {

		Object instance = null;

		if (!parent.isPresent()) {
			instance = dslCache.get(iface);
		}

		if (instance == null) {

			instance = settings.getDslInstantiator().instantiate(iface, classLoader, parent, metadata);

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

	public static HelenusEntity entity(Class<?> iface) {
		Metadata metadata = metadataForEntity.get(iface);
		if (metadata == null) {
			HelenusSession session = session();
			if (session != null) {
				metadata = session.getMetadata();
			}
		}
		return entity(iface, metadata);
	}

	public static HelenusEntity entity(Class<?> iface, Metadata metadata) {

		Object dsl = dsl(iface, metadata);

		DslExportable e = (DslExportable) dsl;

		return e.getHelenusMappingEntity();
	}

	public static HelenusEntity resolve(Object ifaceOrDsl) {
		return resolve(ifaceOrDsl, metadataForEntity.get(ifaceOrDsl));
	}

	public static HelenusEntity resolve(Object ifaceOrDsl, Metadata metadata) {

		if (ifaceOrDsl == null) {
			throw new HelenusMappingException("ifaceOrDsl is null");
		}

		if (ifaceOrDsl instanceof DslExportable) {

			DslExportable e = (DslExportable) ifaceOrDsl;

			return e.getHelenusMappingEntity();
		}

		if (ifaceOrDsl instanceof Class) {

			Class<?> iface = (Class<?>) ifaceOrDsl;

			if (!iface.isInterface()) {
				throw new HelenusMappingException("class is not an interface " + iface);
			}

			metadataForEntity.putIfAbsent(iface, metadata);
			return entity(iface, metadata);
		}

		throw new HelenusMappingException("unknown dsl object or mapping interface " + ifaceOrDsl);
	}
}
