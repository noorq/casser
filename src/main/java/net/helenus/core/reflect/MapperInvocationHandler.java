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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.Map;

import net.helenus.support.HelenusException;

public class MapperInvocationHandler<E> implements InvocationHandler {

	private final Map<String, Object> src;
	private final Class<E> iface;

	public MapperInvocationHandler(Class<E> iface, Map<String, Object> src) {
		this.src = src;
		this.iface = iface;
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        if (method.isDefault()) {
            // NOTE: This is reflection magic to invoke (non-recursively) a default method implemented on an interface
            // that we've proxied (in ReflectionDslInstantiator).  I found the answer in this article.
            // https://zeroturnaround.com/rebellabs/recognize-and-conquer-java-proxies-default-methods-and-method-handles/

            // First, we need an instance of a private inner-class found in MethodHandles.
            Constructor<MethodHandles.Lookup> constructor = MethodHandles.Lookup.class.getDeclaredConstructor(Class.class, int.class);
            constructor.setAccessible(true);

            // Now we need to lookup and invoke special the default method on the interface class.
            final Class<?> declaringClass = method.getDeclaringClass();
            Object result = constructor.newInstance(declaringClass, MethodHandles.Lookup.PRIVATE)
                            .unreflectSpecial(method, declaringClass)
                            .bindTo(proxy)
                            .invokeWithArguments(args);
            return result;
        }

		String methodName = method.getName();

		if ("equals".equals(methodName) && method.getParameterCount() == 1) {
			Object otherObj = args[0];
			if (otherObj == null) {
				return false;
			}
			if (Proxy.isProxyClass(otherObj.getClass())) {
				return this == Proxy.getInvocationHandler(otherObj);
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

		if (MapExportable.TO_MAP_METHOD.equals(methodName)) {
			return Collections.unmodifiableMap(src);
		}

		Object value = src.get(methodName);

        Class<?> returnType = method.getReturnType();

		if (value == null) {

			if (returnType.isPrimitive()) {

				DefaultPrimitiveTypes type = DefaultPrimitiveTypes.lookup(returnType);
				if (type == null) {
					throw new HelenusException("unknown primitive type " + returnType);
				}

				return type.getDefaultValue();

			}  else if (returnType.isEnum()) {
				throw new HelenusException("missing default type for enum user type " + returnType);
			}

		} else if (returnType.isEnum() && (value.getClass() == Integer.class || value.getClass() == int.class)) {
		    try {
                value = Class.forName(returnType.getName()).getEnumConstants()[(Integer) value];
            } catch (ArrayIndexOutOfBoundsException e) {
		        throw new IllegalArgumentException("invalid ordinal " + value + " for enum type " + returnType.getSimpleName());
            }
        }

		return value;
	}

}
