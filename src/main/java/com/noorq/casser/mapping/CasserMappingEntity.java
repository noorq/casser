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

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.noorq.casser.config.CasserSettings;
import com.noorq.casser.core.Casser;
import com.noorq.casser.support.CasserMappingException;

public final class CasserMappingEntity implements CasserEntity {

	private final Class<?> iface;
	private final CasserEntityType type;
	private final IdentityName name;
	private final ImmutableMap<String, CasserProperty> props;
	
	public CasserMappingEntity(Class<?> iface) {
		this(iface, autoDetectType(iface));
	}
	
	public CasserMappingEntity(Class<?> iface, CasserEntityType type) {
		
		if (iface == null || !iface.isInterface()) {
			throw new IllegalArgumentException("invalid parameter " + iface);
		}
		
		this.iface = iface;
		this.type = Objects.requireNonNull(type, "type is empty");
		this.name = resolveName(iface, type);
		
		CasserSettings settings = Casser.settings();
		
		Method[] all = iface.getDeclaredMethods();
		
		ImmutableMap.Builder<String, CasserProperty> propsBuilder = ImmutableMap.<String, CasserProperty>builder();
		
		for (Method m : all) {
			
			if (settings.getGetterMethodDetector().apply(m)) {
				
				CasserProperty prop = new CasserMappingProperty(this, m);
				
				propsBuilder.put(prop.getPropertyName(), prop);
				
			}
			
		}

		this.props = propsBuilder.build();
	}

	@Override
	public CasserEntityType getType() {
		return type;
	}

	@Override
	public Class<?> getMappingInterface() {
		return iface;
	}

	@Override
	public Collection<CasserProperty> getProperties() {
		return props.values();
	}
	
	@Override
	public CasserProperty getProperty(String name) {
		return props.get(name);
	}

	@Override
	public IdentityName getName() {
		return name;
	}
	
	private static IdentityName resolveName(Class<?> iface, CasserEntityType type) {
		
		switch(type) {
		
		case TABLE:
			return MappingUtil.getTableName(iface, true);
			
		case USER_DEFINED_TYPE:
			return MappingUtil.getUserDefinedTypeName(iface, true);
		}

		throw new CasserMappingException("invalid entity type " + type + " in " + type);

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
	
	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append(iface.getSimpleName())
		.append("(").append(name.getName()).append(") ")
		.append(type.name().toLowerCase())
		.append(":\n");
		for (CasserProperty prop : props.values()) {
			String columnName = prop.getColumnName().getName();
			str.append("  ");
			str.append(prop.getDataType());
			str.append(" ");
			str.append(prop.getPropertyName());
			str.append("(");
			if (!columnName.equals(prop.getPropertyName())) {
				str.append(columnName);
			}
			str.append(") ");
			
			if (prop.isPartitionKey()) {
				str.append("partition_key[");
				str.append(prop.getOrdinal());
				str.append("] ");
			}

			if (prop.isClusteringColumn()) {
				str.append("clustering_column[");
				str.append(prop.getOrdinal());
				str.append("] ");
				OrderingDirection od = prop.getOrdering();
				if (od != null) {
					str.append(od.name().toLowerCase()).append(" ");
				}
			}
			
			if (prop.isStatic()) {
				str.append("static ");
			}
			
			Optional<IdentityName> idx = prop.getIndexName();
			if (idx.isPresent()) {
				str.append("index(").append(idx.get().getName()).append(") "); 
			}
			
			str.append("\n");
		}
		return str.toString();
	}
	
}
