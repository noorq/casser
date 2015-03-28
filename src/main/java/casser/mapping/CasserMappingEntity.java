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

public class CasserMappingEntity implements CasserEntity {

	private final Class<?> iface;
	private final CasserEntityType type;
	private String name;
	private final Map<String, CasserMappingProperty> props = new HashMap<String, CasserMappingProperty>();
	
	public CasserMappingEntity(Class<?> iface) {
		this(iface, autoDetectType(iface));
	}
	
	public CasserMappingEntity(Class<?> iface, CasserEntityType type) {
		
		if (iface == null || !iface.isInterface()) {
			throw new IllegalArgumentException("invalid parameter " + iface);
		}
		
		this.iface = iface;
		this.type = Objects.requireNonNull(type, "type is empty");
		
		CasserSettings settings = Casser.settings();
		
		Method[] all = iface.getDeclaredMethods();
		
		for (Method m : all) {
			
			if (settings.getGetterMethodDetector().apply(m)) {
				
				CasserMappingProperty prop = new CasserMappingProperty(this, m);
				
				props.put(prop.getPropertyName(), prop);
				
			}
			
		}

	}

	@Override
	public CasserEntityType getType() {
		return type;
	}

	public Class<?> getMappingInterface() {
		return iface;
	}	
	
	@Override
	public String toString() {
		return iface.toString();
	}

	@Override
	public Collection<CasserProperty> getProperties() {
		return Collections.unmodifiableCollection(props.values());
	}
	
	@Override
	public CasserProperty getProperty(String name) {
		return props.get(name);
	}

	public Collection<CasserMappingProperty> getMappingProperties() {
		return Collections.unmodifiableCollection(props.values());
	}
	
	public CasserMappingProperty getMappingProperty(String name) {
		return props.get(name);
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

		throw new CasserMappingException("entity must be annotated by @Table or @UserDefinedType " + iface);
	}
	
}
