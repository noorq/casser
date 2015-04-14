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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.noorq.casser.mapping.CasserEntity;
import com.noorq.casser.mapping.CasserEntityType;
import com.noorq.casser.mapping.CasserProperty;
import com.noorq.casser.mapping.type.AbstractDataType;
import com.noorq.casser.mapping.type.UDTDataType;
import com.noorq.casser.support.CasserMappingException;

public final class SessionRepositoryBuilder {

	private static final Optional<CasserEntityType> OPTIONAL_UDT = Optional.of(CasserEntityType.USER_DEFINED_TYPE);
	
	private final Map<Class<?>, CasserEntity> entityMap = new HashMap<Class<?>, CasserEntity>();

	private final Map<String, UserType> userTypeMap = new HashMap<String, UserType>();

    private final Multimap<CasserEntity, CasserEntity> userTypeUsesMap = HashMultimap.create(); 
	
	public SessionRepository build() {
		return new SessionRepository(this);
	}

	public Collection<CasserEntity> getUserTypeUses(CasserEntity udtName) {
		return userTypeUsesMap.get(udtName);
	}

	public Collection<CasserEntity> entities() {
		return entityMap.values();
	}
	
	protected Map<Class<?>, CasserEntity> getEntityMap() {
		return entityMap;
	}

	protected Map<String, UserType> getUserTypeMap() {
		return userTypeMap;
	}

	public void addUserType(String name, UserType userType) {
		userTypeMap.putIfAbsent(name.toLowerCase(), userType);
	}

	public CasserEntity add(Object dsl) {
		return add(dsl, Optional.empty());
	}
	
	public void addEntity(CasserEntity entity) {

		CasserEntity concurrentEntity = entityMap.putIfAbsent(entity.getMappingInterface(), entity);
		
		if (concurrentEntity == null) {
			addUserDefinedTypes(entity.getOrderedProperties());
		}
			
	}
	
	public CasserEntity add(Object dsl, Optional<CasserEntityType> type) {

		CasserEntity casserEntity = Casser.resolve(dsl);
		
		Class<?> iface = casserEntity.getMappingInterface();
		
		CasserEntity entity = entityMap.get(iface);
		
		if (entity == null) {

			entity = casserEntity;
			
			if (type.isPresent() && entity.getType() != type.get()) {
				throw new CasserMappingException("unexpected entity type " + entity.getType() + " for " + entity);
			}
			
			CasserEntity concurrentEntity = entityMap.putIfAbsent(iface, entity);
					
			if (concurrentEntity == null) {
				addUserDefinedTypes(entity.getOrderedProperties());
			}
			else {
				entity = concurrentEntity;
			}

		}
		
		return entity;
	}
	
	private void addUserDefinedTypes(Collection<CasserProperty> props) {
		
		for (CasserProperty prop : props) {
			
			AbstractDataType type = prop.getDataType();
			
			if (!UDTValue.class.isAssignableFrom(prop.getJavaType())) {
				
				for (Class<?> udtClass : type.getUdtClasses()) {
					
					CasserEntity addedUserType = add(udtClass, OPTIONAL_UDT);
					
					if (CasserEntityType.USER_DEFINED_TYPE == prop.getEntity().getType()) {
						userTypeUsesMap.put(prop.getEntity(), addedUserType);
					}
					
				}
				
			}
			
		}
		
	}
	
}
