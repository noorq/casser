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
package casser.mapping;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import casser.support.CasserException;
import casser.support.CasserMappingException;

public class CasserMappingRepository {

	private final Map<Class<?>, CasserMappingEntity<?>> entityMap = new HashMap<Class<?>, CasserMappingEntity<?>>();

	private final Map<String, CasserMappingEntity<?>> udtMap = new HashMap<String, CasserMappingEntity<?>>();

	private boolean readOnly = false;
	
	public CasserMappingRepository setReadOnly() {
		this.readOnly = true;
		return this;
	}
	
	public void addUserType(String name, Class<?> userTypeClass) {
		
		if (readOnly) {
			throw new CasserException("read-only mode");
		}
		
		udtMap.putIfAbsent(name, new CasserMappingEntity(userTypeClass));
		
	}
	
	public Map<String, CasserMappingEntity<?>> knownUserTypes() {
		return Collections.unmodifiableMap(udtMap);
	}
	
	public CasserMappingEntity<?> findUserType(Class<?> userTypeClass) {
		return udtMap.get(userTypeClass);
	}
	
	public void addEntity(Object dsl) {
		
		if (readOnly) {
			throw new CasserException("read-only mode");
		}

		Class<?> iface = MappingUtil.getMappingInterface(dsl);
			
		entityMap.putIfAbsent(iface, new CasserMappingEntity(iface));
		
	}
	
	public Collection<CasserMappingEntity<?>> knownEntities() {
		return Collections.unmodifiableCollection(entityMap.values());
	}
	
	public CasserMappingEntity<?> getEntity(Class<?> iface) {
		
		CasserMappingEntity<?> entity = entityMap.get(iface);
		
		if (entity == null) {
			throw new CasserMappingException("please add all entities in SessionInitializer, unknown entity interface " + iface);
		}
		
		return entity;
	}
	
}
