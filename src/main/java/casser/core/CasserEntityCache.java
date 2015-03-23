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
package casser.core;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import casser.mapping.CasserMappingEntity;

public class CasserEntityCache {

	private ConcurrentMap<Class<?>, CasserMappingEntity<?>> cache = new ConcurrentHashMap<Class<?>, CasserMappingEntity<?>>();

	public CasserMappingEntity<?> getEntity(Class<?> iface) {
		
		CasserMappingEntity<?> entity = cache.get(iface);
		
		if (entity == null) {
			entity = new CasserMappingEntity(iface);
			
			CasserMappingEntity<?> c = cache.putIfAbsent(iface, entity);
			if (c != null) {
				entity = c;
			}
		}
		
		return entity;
	}
	
}
