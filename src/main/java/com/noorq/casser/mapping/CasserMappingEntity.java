/*
 *
 *      Copyright (C) 2015 The Casser Authors
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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.noorq.casser.config.CasserSettings;
import com.noorq.casser.core.Casser;
import com.noorq.casser.mapping.annotation.Table;
import com.noorq.casser.mapping.annotation.Tuple;
import com.noorq.casser.mapping.annotation.UDT;
import com.noorq.casser.support.CasserMappingException;

public final class CasserMappingEntity implements CasserEntity {

	private final Class<?> iface;
	private final CasserEntityType type;
	private final IdentityName name;
	private final ImmutableMap<String, CasserProperty> props;
	private final ImmutableList<CasserProperty> orderedProps;
	
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
		
		List<CasserProperty> propsLocal = new ArrayList<CasserProperty>();
		ImmutableMap.Builder<String, CasserProperty> propsBuilder = ImmutableMap.builder();
		
		for (Method m : all) {
			
			if (settings.getGetterMethodDetector().apply(m)) {
				
				CasserProperty prop = new CasserMappingProperty(this, m);
				
				propsBuilder.put(prop.getPropertyName(), prop);
				propsLocal.add(prop);

			}
			
		}
		
		this.props = propsBuilder.build();
		
		Collections.sort(propsLocal, TypeAndOrdinalColumnComparator.INSTANCE);
		this.orderedProps = ImmutableList.copyOf(propsLocal);

		validateOrdinals();
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
	public Collection<CasserProperty> getOrderedProperties() {
		return orderedProps;
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
			
		case TUPLE:
			return IdentityName.of(MappingUtil.getDefaultEntityName(iface), false);
			
		case UDT:
			return MappingUtil.getUserDefinedTypeName(iface, true);
		}

		throw new CasserMappingException("invalid entity type " + type + " in " + type);

	}
	
	private static CasserEntityType autoDetectType(Class<?> iface) {
		
		Objects.requireNonNull(iface, "empty iface");
		
		if (null != iface.getDeclaredAnnotation(Table.class)) {
			return CasserEntityType.TABLE;
		}
		
		else if (null != iface.getDeclaredAnnotation(Tuple.class)) {
			return CasserEntityType.TUPLE;
		}

		else if (null != iface.getDeclaredAnnotation(UDT.class)) {
			return CasserEntityType.UDT;
		}
		
		throw new CasserMappingException("entity must be annotated by @Table or @Tuple or @UserDefinedType " + iface);
	}

	private void validateOrdinals() {
		
		switch(getType()) {
		
		case TABLE:
			validateOrdinalsForTable();
			break;
			
		case TUPLE:
			validateOrdinalsInTuple();
			break;
			
		default:
			break;
		}
		
	}
	
	private void validateOrdinalsForTable() {
		
		BitSet partitionKeys = new BitSet();
		BitSet clusteringColumns = new BitSet();

		for (CasserProperty prop : getOrderedProperties()) {
			
			ColumnType type = prop.getColumnType();
			
			int ordinal = prop.getOrdinal();

			switch(type) {
			
			case PARTITION_KEY:
				if (partitionKeys.get(ordinal)) {
					throw new CasserMappingException("detected two or more partition key columns with the same ordinal " + ordinal + " in " + prop.getEntity());
				}
				partitionKeys.set(ordinal);
				break;
				
			case CLUSTERING_COLUMN:
				if (clusteringColumns.get(ordinal)) {
					throw new CasserMappingException("detected two or clustering columns with the same ordinal " + ordinal + " in " + prop.getEntity());
				}
				clusteringColumns.set(ordinal);
				break;
				
			default:
				break;	
			}
		
		}
		
	}
	
	private void validateOrdinalsInTuple() {
		boolean[] ordinals = new boolean[props.size()];
		
		getOrderedProperties().forEach(p -> {
			
			int ordinal = p.getOrdinal();
			
			if (ordinal < 0 || ordinal >= ordinals.length) {
				throw new CasserMappingException("invalid ordinal " + ordinal + " found for property " + p.getPropertyName() + " in " + p.getEntity());
			}

			if (ordinals[ordinal]) {
				throw new CasserMappingException("detected two or more properties with the same ordinal " + ordinal + " in " + p.getEntity());
			}
			
			ordinals[ordinal] = true;
			
		});
		
		for (int i = 0; i != ordinals.length; ++i) {
			if (!ordinals[i]) {
				throw new CasserMappingException("detected absent ordinal " + i + " in " + this);
			}
		}
		
	}
	
	@Override
	public String toString() {
		
		StringBuilder str = new StringBuilder();
		str.append(iface.getSimpleName())
		.append("(").append(name.getName()).append(") ")
		.append(type.name().toLowerCase())
		.append(":\n");
		
		for (CasserProperty prop : getOrderedProperties()) {
			str.append(prop.toString());
			str.append("\n");
		}
		return str.toString();
	}
	
}
