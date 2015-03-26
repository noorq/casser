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

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import casser.config.CasserSettings;
import casser.core.Casser;
import casser.support.CasserMappingException;

public class CasserMappingEntity<E> implements CasserEntity<E> {

	private final Class<E> iface;
	private final CasserEntityType type;
	private String name;
	private final Map<String, CasserMappingProperty<E>> props = new HashMap<String, CasserMappingProperty<E>>();
	
	public CasserMappingEntity(Class<E> iface) {
		this(iface, autoDetectType(iface));
	}
	
	public CasserMappingEntity(Class<E> iface, CasserEntityType type) {
		
		if (iface == null || !iface.isInterface()) {
			throw new IllegalArgumentException("invalid parameter " + iface);
		}
		
		this.iface = iface;
		this.type = Objects.requireNonNull(type, "type is empty");
		
		CasserSettings settings = Casser.settings();
		
		Method[] all = iface.getDeclaredMethods();
		
		for (Method m : all) {
			
			if (settings.getGetterMethodDetector().apply(m)) {
				
				CasserMappingProperty<E> prop = new CasserMappingProperty<E>(this, m);
				
				props.put(prop.getPropertyName(), prop);
				
			}
			
		}

		for (Method m : all) {
			
			if (settings.getSetterMethodDetector().apply(m)) {
				
				String propertyName = MappingUtil.getPropertyName(m);

				CasserMappingProperty<E> prop = props.get(propertyName);
				
				if (prop != null) {
					prop.setSetterMethod(m);
				}
				
			}
			
		}

	}

	@Override
	public CasserEntityType getType() {
		return type;
	}

	public Class<E> getMappingInterface() {
		return iface;
	}	
	
	@Override
	public String toString() {
		return iface.toString();
	}

	@Override
	public Collection<CasserProperty<E>> getProperties() {
		return Collections.unmodifiableCollection(props.values());
	}

	public Collection<CasserMappingProperty<E>> getMappingProperties() {
		return Collections.unmodifiableCollection(props.values());
	}

	@Override
	public String getName() {
		
		if (name == null) {
			
			switch(type) {
			
			case TABLE:
				name = MappingUtil.getTableName(iface, true);
				break;
				
			case USER_DEFINED_TYPE:
				name = MappingUtil.getUserDefinedTypeName(iface, true);
				break;
			}
			
		}
		
		return name;
	}
	
	private static CasserEntityType autoDetectType(Class<?> iface) {
		
		Objects.requireNonNull(iface, "empty iface");
		
		if (null != iface.getDeclaredAnnotation(Table.class)) {
			return CasserEntityType.TABLE;
		}

		else if (null != iface.getDeclaredAnnotation(UserDefinedType.class)) {
			return CasserEntityType.USER_DEFINED_TYPE;
		}

		throw new CasserMappingException("unknown entity type " + iface);
	}
	
}
