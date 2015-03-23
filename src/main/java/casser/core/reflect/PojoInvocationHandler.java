/*
 *      Copyright (C) 2015 Noorq Inc.
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
package casser.core.reflect;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import casser.config.CasserSettings;
import casser.core.Casser;
import casser.support.CasserException;

public class PojoInvocationHandler<E> implements InvocationHandler {

	private final Class<E> iface;
	
	private final Map<Method, Operation> methodToIndex = new HashMap<Method, Operation>();
    private final Object[] values;
	
	public PojoInvocationHandler(Class<E> iface) {
		this.iface = iface;
		
		int properties = initializeMethods();
		
		this.values = new Object[properties];
	}
	
	@Override
	public Object invoke(Object proxy, Method method, Object[] args)
			throws Throwable {
		
		if ("toString".equals(method.getName())) {
			return "Casser Pojo for " + iface;
		}
		
		Operation op = methodToIndex.get(method);
		
		if (op == null) {
			throw new CasserException("method not found " + method);
		}
		
		switch(op.type) {
		
		case GET:
			Object val = values[op.index];
			Class<?> returnType = method.getReturnType();
			
			if (val == null && returnType.isPrimitive()) {
				
				DefaultPrimitiveTypes type = DefaultPrimitiveTypes.lookup(returnType);
				if (type == null) {
					throw new CasserException("unknown primitive type " + returnType);
				}
				
				return type.getDefaultValue();
			}
			return val;
		case SET:
			values[op.index] = args[0];
			break;
		
		}
		
		return null;
	}
	
	private int initializeMethods() {
		
		Map<String, Integer> props = new HashMap<String, Integer>();
		
		CasserSettings settings = Casser.settings();
		
		for (Method declaredMethod : iface.getDeclaredMethods()) {
			
			String propName = settings.getMethodNameToPropertyConverter().apply(declaredMethod.getName()).toLowerCase();
			
			int index = props.size();
			Integer prev = props.putIfAbsent(propName, props.size());
			if (prev != null) {
				index = prev;
			}
			
			OperationType type = getOperationType(declaredMethod);
			
			validateMethod(declaredMethod, type);
			
			methodToIndex.put(declaredMethod, new Operation(type, index));
			
		}
	
		return props.size();
	}
	
	private void validateMethod(Method method, OperationType type) {
		
		switch(type) {
		
		case GET:
			if (method.getParameterCount() != 0 || method.getReturnType() == void.class) {
				throw new CasserException("invalid getter method " + method);
			}
			break;
			
		case SET:
			if (method.getParameterCount() != 1 || method.getReturnType() != void.class) {
				throw new CasserException("invalid setter method " + method);
			}
			break;
		
		}
		
	}
	
	private OperationType getOperationType(Method method) {
		
		String name = method.getName();
		
		if (name.startsWith("get") || name.startsWith("is") || name.startsWith("has")) {
			return OperationType.GET;
		}
		
		if (name.startsWith("set")) {
			return OperationType.SET;
		}
		
		throw new CasserException("invalid pojo method " + method);
		
	}
	
	private enum OperationType {
		GET, SET;
	}
	
	private static final class Operation {
		
		private final OperationType type;
		private final int index;
		
		private Operation(OperationType type, int index) {
			this.type = type;
			this.index = index;
		}
		
	}
	
}
