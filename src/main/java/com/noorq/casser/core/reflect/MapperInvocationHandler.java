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
package com.noorq.casser.core.reflect;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;

import com.noorq.casser.support.CasserException;

public class MapperInvocationHandler<E> implements InvocationHandler {

	private final Map<String, Object> src;
	private final Class<E> iface;
	
	public MapperInvocationHandler(Class<E> iface, Map<String, Object> src) {
		this.src = src;
		this.iface = iface;
	}
	
	@Override
	public Object invoke(Object proxy, Method method, Object[] args)
			throws Throwable {
		
		if (method.getParameterCount() != 0 || method.getReturnType() == void.class) {
			throw new CasserException("invalid getter method " + method);
		}
		
		String methodName = method.getName();
		
		if ("toString".equals(methodName)) {
			return iface.getSimpleName() + ": " + src.toString();
		}

		if (MapExportable.TO_MAP_METHOD.equals(methodName)) {
			return Collections.unmodifiableMap(src);
		}

		Object value = src.get(methodName);
		
		if (value == null) {
			
			Class<?> returnType = method.getReturnType();
			
			if (returnType.isPrimitive()) {
				
				DefaultPrimitiveTypes type = DefaultPrimitiveTypes.lookup(returnType);
				if (type == null) {
					throw new CasserException("unknown primitive type " + returnType);
				}
				
				return type.getDefaultValue();
				
			}
			
		}
		
		return value;
	}
	
}
