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
import java.lang.reflect.Type;
import java.util.Optional;
import java.util.function.Function;

import com.noorq.casser.core.SessionRepository;
import com.noorq.casser.mapping.javatype.AbstractJavaType;
import com.noorq.casser.mapping.javatype.KnownJavaTypes;
import com.noorq.casser.mapping.type.AbstractDataType;

public final class CasserMappingProperty implements CasserProperty {

	private final CasserEntity entity; 
	private final Method getter;
	
	private final String propertyName;
	private final Optional<IdentityName> indexName;

	private final ColumnInformation columnInfo;
	
	private final Type genericJavaType;
	private final Class<?> javaType;
	private final AbstractJavaType abstractJavaType;
	private final AbstractDataType dataType;
		
	private volatile Optional<Function<Object, Object>> readConverter = null;
	private volatile Optional<Function<Object, Object>> writeConverter = null;
	
	public CasserMappingProperty(CasserMappingEntity entity, Method getter) {
		this.entity = entity;
		this.getter = getter;
		
		this.propertyName = MappingUtil.getPropertyName(getter);
		this.indexName = MappingUtil.getIndexName(getter);
		
		this.columnInfo = new ColumnInformation(getter);
	
		this.genericJavaType = getter.getGenericReturnType();
		this.javaType = getter.getReturnType();
		this.abstractJavaType = KnownJavaTypes.resolveJavaType(this.javaType);

		this.dataType = abstractJavaType.resolveDataType(this.getter, this.genericJavaType, this.columnInfo.getColumnType());
	}
	
	@Override
	public CasserEntity getEntity() {
		return entity;
	}

	@Override
	public Class<?> getJavaType() {
		return (Class<?>) javaType;
	}
	
	@Override
	public AbstractDataType getDataType() {
		return dataType;
	}

	@Override
	public ColumnType getColumnType() {
		return columnInfo.getColumnType();
	}
	
	@Override
	public int getOrdinal() {
		return columnInfo.getOrdinal();
	}

	@Override
	public OrderingDirection getOrdering() {
		return columnInfo.getOrdering();
	}

	@Override
	public IdentityName getColumnName() {
		return columnInfo.getColumnName();
	}

	@Override
	public Optional<IdentityName> getIndexName() {
		return indexName;
	}
	
	@Override
	public String getPropertyName() {
		return propertyName;
	}

	@Override
	public Method getGetterMethod() {
		return getter;
	}
	
	@Override
	public Optional<Function<Object, Object>> getReadConverter(SessionRepository repository) {

		if (readConverter == null) {
			readConverter = abstractJavaType.resolveReadConverter(this.dataType, repository);
		}
		
		return readConverter;
	}
	
	@Override
	public Optional<Function<Object, Object>> getWriteConverter(SessionRepository repository) {
		
		if (writeConverter == null) {
			writeConverter = abstractJavaType.resolveWriteConverter(this.dataType, repository);
		}
		
		return writeConverter;
	}

}
