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
package com.noorq.casser.mapping;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.noorq.casser.support.Either;

public final class MappingRepositoryBuilder {

	private static final Optional<CasserEntityType> OPTIONAL_UDT = Optional.of(CasserEntityType.USER_DEFINED_TYPE);
	
	private final Map<Class<?>, CasserMappingEntity> entityMap = new HashMap<Class<?>, CasserMappingEntity>();

	private final Map<String, UserType> userTypeMap = new HashMap<String, UserType>();

    private final Multimap<CasserMappingEntity, CasserMappingEntity> userTypeUsesMap = HashMultimap.create(); 
	
	public CasserMappingRepository build() {
		return new CasserMappingRepository(this);
	}

	public Collection<CasserMappingEntity> getUserTypeUses(CasserMappingEntity udtName) {
		return userTypeUsesMap.get(udtName);
	}

	public Collection<CasserMappingEntity> entities() {
		return entityMap.values();
	}
	
	protected Map<Class<?>, CasserMappingEntity> getEntityMap() {
		return entityMap;
	}

	protected Map<String, UserType> getUserTypeMap() {
		return userTypeMap;
	}

	public void addUserType(String name, UserType userType) {
		userTypeMap.putIfAbsent(name.toLowerCase(), userType);
	}

	public CasserMappingEntity add(Object dsl) {
		return add(dsl, Optional.empty());
	}
	
	public CasserMappingEntity add(Object dsl, Optional<CasserEntityType> type) {

		Class<?> iface = MappingUtil.getMappingInterface(dsl);
		
		CasserMappingEntity entity = entityMap.get(iface);
		
		if (entity == null) {

			entity = type.isPresent() ? 
					new CasserMappingEntity(iface, type.get()) :
						new CasserMappingEntity(iface);

			CasserMappingEntity concurrentEntity = entityMap.putIfAbsent(iface, entity);
					
			if (concurrentEntity == null) {
				addUserDefinedTypes(entity.getMappingProperties());
			}
			else {
				entity = concurrentEntity;
			}

		}
		
		return entity;
	}
	
	private void addUserDefinedTypes(Collection<CasserMappingProperty> props) {
		
		for (CasserMappingProperty prop : props) {
			
			Either<DataType, IdentityName> type = prop.getColumnType();
			
			if (type.isRight() && !UDTValue.class.isAssignableFrom(prop.getJavaType())) {
				
				CasserMappingEntity addedUserType = add(prop.getJavaType(), OPTIONAL_UDT);
				
				if (CasserEntityType.USER_DEFINED_TYPE == prop.getEntity().getType()) {
					userTypeUsesMap.put(prop.getEntity(), addedUserType);
				}
			}
			
		}
		
	}
	
}
