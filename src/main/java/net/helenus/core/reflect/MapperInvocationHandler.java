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
package net.helenus.core.reflect;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import net.helenus.core.Helenus;
import net.helenus.mapping.annotation.Transient;
import net.helenus.mapping.value.ValueProviderMap;
import net.helenus.support.HelenusException;

public class MapperInvocationHandler<E> implements InvocationHandler, Serializable {
	private static final long serialVersionUID = -7044209982830584984L;

	private final Map<String, Object> src;
	private final Class<E> iface;

	public MapperInvocationHandler(Class<E> iface, Map<String, Object> src) {
		this.src = src;
		this.iface = iface;
	}

	private Object invokeDefault(Object proxy, Method method, Object[] args) throws Throwable {
		// NOTE: This is reflection magic to invoke (non-recursively) a default method
		// implemented on an interface
		// that we've proxied (in ReflectionDslInstantiator). I found the answer in this
		// article.
		// https://zeroturnaround.com/rebellabs/recognize-and-conquer-java-proxies-default-methods-and-method-handles/

		// First, we need an instance of a private inner-class found in MethodHandles.
		Constructor<MethodHandles.Lookup> constructor = MethodHandles.Lookup.class.getDeclaredConstructor(Class.class,
				int.class);
		constructor.setAccessible(true);

		// Now we need to lookup and invoke special the default method on the interface
		// class.
		final Class<?> declaringClass = method.getDeclaringClass();
		Object result = constructor.newInstance(declaringClass, MethodHandles.Lookup.PRIVATE)
				.unreflectSpecial(method, declaringClass).bindTo(proxy).invokeWithArguments(args);
		return result;
	}

	private Object writeReplace() {
		return new SerializationProxy<E>(this);
	}
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Proxy required.");
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

		// Transient, default methods should simply be invoked as-is.
		if (method.isDefault() && method.getDeclaredAnnotation(Transient.class) != null) {
			return invokeDefault(proxy, method, args);
		}

		String methodName = method.getName();

		if ("equals".equals(methodName) && method.getParameterCount() == 1) {
			Object otherObj = args[0];
			if (otherObj == null) {
				return false;
			}
			if (Proxy.isProxyClass(otherObj.getClass())) {
				if (this == Proxy.getInvocationHandler(otherObj)) {
					return true;
				}
			}
			if (otherObj instanceof MapExportable && src.equals(((MapExportable) otherObj).toMap())) {
				return true;
			}
			return false;
		}

		if (method.getParameterCount() != 0 || method.getReturnType() == void.class) {
			throw new HelenusException("invalid getter method " + method);
		}

		if ("hashCode".equals(methodName)) {
			return hashCode();
		}

		if ("toString".equals(methodName)) {
			return iface.getSimpleName() + ": " + src.toString();
		}

		if ("writeReplace".equals(methodName)) {
			return new SerializationProxy(this);
		}

		if ("readObject".equals(methodName)) {
			throw new InvalidObjectException("Proxy required.");
		}

		if ("dsl".equals(methodName)) {
			return Helenus.dsl(iface);
		}

		if (MapExportable.TO_MAP_METHOD.equals(methodName)) {
			return src; // return Collections.unmodifiableMap(src);
		}

		Object value = src.get(methodName);

		Class<?> returnType = method.getReturnType();

		if (value == null) {

			// Default implementations of non-Transient methods in entities are the default
			// value when the
			// map contains 'null'.
			if (method.isDefault()) {
				return invokeDefault(proxy, method, args);
			}

			// Otherwise, if the return type of the method is a primitive Java type then
			// we'll return the standard
			// default values to avoid a NPE in user code.
			if (returnType.isPrimitive()) {
				DefaultPrimitiveTypes type = DefaultPrimitiveTypes.lookup(returnType);
				if (type == null) {
					throw new HelenusException("unknown primitive type " + returnType);
				}
				return type.getDefaultValue();
			}
		}

		return value;
	}

	static class SerializationProxy<E> implements Serializable {

		private static final long serialVersionUID = -5617583940055969353L;

		private final Class<E> iface;
		private final Map<String, Object> src;

		public SerializationProxy(MapperInvocationHandler mapper) {
			this.iface = mapper.iface;
			if (mapper.src instanceof ValueProviderMap) {
				this.src = new HashMap<String, Object>(mapper.src.size());
				Set<String> keys = mapper.src.keySet();
				for (String key : keys) {
					this.src.put(key, mapper.src.get(key));
				}
			} else {
				this.src = mapper.src;
			}
		}

		Object readResolve() throws ObjectStreamException {
			return new MapperInvocationHandler(iface, src);
		}

	}
}
