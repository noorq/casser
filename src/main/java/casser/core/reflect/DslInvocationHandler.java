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
package casser.core.reflect;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import casser.mapping.CasserMappingEntity;
import casser.mapping.CasserMappingProperty;
import casser.support.CasserException;
import casser.support.DslPropertyException;

public class DslInvocationHandler<E> implements InvocationHandler {

	private final CasserMappingEntity<E> entity;
	
	private final Map<Method, CasserMappingProperty<E>> map = new HashMap<Method, CasserMappingProperty<E>>();
	
	public DslInvocationHandler(Class<E> iface) {
		
		this.entity = new CasserMappingEntity<E>(iface);
		
		for (CasserMappingProperty<E> prop : entity.getMappingProperties()) {
			
			map.put(prop.getGetterMethod(), prop);
			
			Method setter = prop.getSetterMethod();
			
			if (setter != null) {
				map.put(setter, prop);
			}
			
		}
	}
	
	@Override
	public Object invoke(Object proxy, Method method, Object[] args)
			throws Throwable {
		
		if ("toString".equals(method.getName())) {
			return "Casser Dsl for " + entity.getMappingInterface();
		}
		
		CasserMappingProperty<E> prop = map.get(method);
		
		if (prop != null) {
			throw new DslPropertyException(prop);	
		}
		
		throw new CasserException("invalid method call " + method);
	}

}
