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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.noorq.casser.core.Casser;
import com.noorq.casser.mapping.CasserMappingEntity;
import com.noorq.casser.mapping.CasserMappingProperty;
import com.noorq.casser.support.CasserException;
import com.noorq.casser.support.CasserMappingException;
import com.noorq.casser.support.DslPropertyException;

public class DslInvocationHandler<E> implements InvocationHandler {

	private final CasserMappingEntity entity;
	private final Optional<CasserPropertyNode> parent;
	
	private final Map<Method, CasserMappingProperty> map = new HashMap<Method, CasserMappingProperty>();
	
	private final Map<Method, Object> udtMap = new HashMap<Method, Object>();

	public DslInvocationHandler(Class<E> iface, ClassLoader classLoader, Optional<CasserPropertyNode> parent) {
		
		this.entity = new CasserMappingEntity(iface);
		this.parent = parent;
		
		for (CasserMappingProperty prop : entity.getMappingProperties()) {
			
			map.put(prop.getGetterMethod(), prop);
			
			if (prop.getUDTType() != null) {
				
				Object childDsl = Casser.dsl(prop.getJavaType(), classLoader,
						Optional.of(new CasserPropertyNode(prop, parent)));
				
				udtMap.put(prop.getGetterMethod(), childDsl);
			}
			
		}
	}
	
	@Override
	public Object invoke(Object proxy, Method method, Object[] args)
			throws Throwable {
		
		if ("toString".equals(method.getName())) {
			return "Dsl:" + entity.getMappingInterface();
		}
		
		CasserMappingProperty prop = map.get(method);
		
		if (prop != null) {
			
			if (prop.getUDTType() != null) {
				
				Object childDsl = udtMap.get(method);
				
				if (childDsl != null) {
					return childDsl;
				}
				else {
					throw new CasserMappingException("childDsl not found for " + method);
				}
				
			}
			
			throw new DslPropertyException(new CasserPropertyNode(prop, parent));	
		}
		
		throw new CasserException("invalid method call " + method);
	}

}
