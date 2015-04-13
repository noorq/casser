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
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.noorq.casser.core.SessionRepository;
import com.noorq.casser.mapping.convert.DateToTimeUUIDConverter;
import com.noorq.casser.mapping.convert.EntityToUDTValueConverter;
import com.noorq.casser.mapping.convert.EnumToStringConverter;
import com.noorq.casser.mapping.convert.StringToEnumConverter;
import com.noorq.casser.mapping.convert.TimeUUIDToDateConverter;
import com.noorq.casser.mapping.convert.TypedConverter;
import com.noorq.casser.mapping.convert.UDTValueToEntityConverter;
import com.noorq.casser.mapping.type.AbstractDataType;
import com.noorq.casser.mapping.type.DTDataType;
import com.noorq.casser.mapping.type.UDTDataType;
import com.noorq.casser.support.CasserMappingException;

public final class CasserMappingProperty implements CasserProperty {

	private final CasserEntity entity; 
	private final Method getter;
	
	private final String propertyName;
	private final IdentityName columnName;
	private final Optional<IdentityName> indexName;

	private final boolean isStatic;
	private final KeyInformation keyInfo;

	private final ColumnType columnType;
	
	private final Type genericJavaType;
	private final Class<?> javaType;
	private final AbstractDataType dataType;
		
	private volatile Optional<Function<Object, Object>> readConverter = null;
	private volatile Optional<Function<Object, Object>> writeConverter = null;
	
	public CasserMappingProperty(CasserMappingEntity entity, Method getter) {
		this.entity = entity;
		this.getter = getter;
		
		this.propertyName = MappingUtil.getPropertyName(getter);
		this.columnName = MappingUtil.getColumnName(getter);
		this.indexName = MappingUtil.getIndexName(getter);
		this.isStatic = MappingUtil.isStaticColumn(getter);
		this.keyInfo = new KeyInformation(getter);

		this.columnType = resolveColumnType(keyInfo, isStatic);

		this.genericJavaType = getter.getGenericReturnType();
		this.javaType = getter.getReturnType();

		this.dataType = resolveAbstractDataType(getter, this.genericJavaType, this.javaType, this.columnType);

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
	public boolean isPartitionKey() {
		return keyInfo.isPartitionKey();
	}

	@Override
	public boolean isClusteringColumn() {
		return keyInfo.isClusteringColumn();
	}

	@Override
	public int getOrdinal() {
		return keyInfo.getOrdinal();
	}

	@Override
	public OrderingDirection getOrdering() {
		return keyInfo.getOrdering();
	}
	
	@Override
	public boolean isStatic() {
		return isStatic;
	}

	@Override
	public IdentityName getColumnName() {
		return columnName;
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
			readConverter = Optional.ofNullable(resolveReadConverter(repository));
		}
		
		return readConverter;
	}

	private Function<Object, Object> resolveReadConverter(SessionRepository repository) {
		
		AbstractDataType abstractDataType = getDataType();
		
		if (abstractDataType instanceof UDTDataType) {
			
			Class<Object> javaType = (Class<Object>) getJavaType();
			
			if (UDTValue.class.isAssignableFrom(javaType)) {
				return null;
			}
			
			return TypedConverter.create(
					UDTValue.class,
					javaType,
					new UDTValueToEntityConverter(javaType, repository));

		}
		else if (abstractDataType instanceof DTDataType) {
		
			Class<?> propertyType = getJavaType();
	
			if (Enum.class.isAssignableFrom(propertyType)) {
				return TypedConverter.create(
						String.class, 
						Enum.class, 
						new StringToEnumConverter(propertyType));
			}
	
			DataType dataType = ((DTDataType) abstractDataType).getDataType();
	
			if (dataType.getName() == DataType.Name.TIMEUUID && propertyType == Date.class) {
				return TypedConverter.create(
						UUID.class, 
						Date.class, 
						TimeUUIDToDateConverter.INSTANCE);			
			}
	
			return null;
		}
		
		return null;
	}
	
	@Override
	public Optional<Function<Object, Object>> getWriteConverter(SessionRepository repository) {
		
		if (writeConverter == null) {
			writeConverter = Optional.ofNullable(resolveWriteConverter(repository));
		}
		
		return writeConverter;
	}
	
	private Function<Object, Object> resolveWriteConverter(SessionRepository repository) {
	
		AbstractDataType abstractDataType = getDataType();
		
		if (abstractDataType instanceof UDTDataType) {

			UDTDataType udtDataType = (UDTDataType) abstractDataType;
			
			if (isUDTValue(javaType)) {
				return null;
			}
			
			Class<Object> javaType = (Class<Object>) getJavaType();
			
			UserType userType = repository.findUserType(udtDataType.getUdtName().getName());
			if (userType == null) {
				throw new CasserMappingException("UserType not found for " + udtDataType.getUdtName() + " with type " + javaType);
			}
			return TypedConverter.create(
					javaType, 
					UDTValue.class, 
					new EntityToUDTValueConverter(javaType, userType, repository));

		}
		else if (abstractDataType instanceof DTDataType) {
		
			Class<?> javaType = getJavaType();
	
			if (Enum.class.isAssignableFrom(javaType)) {
				
				return TypedConverter.create(
						Enum.class, 
						String.class, 
						EnumToStringConverter.INSTANCE);
				
			}
	
			DataType dataType = ((DTDataType) abstractDataType).getDataType();
	
			if (dataType.getName() == DataType.Name.TIMEUUID && javaType == Date.class) {
				
				return TypedConverter.create(
						Date.class, 
						UUID.class, 
						DateToTimeUUIDConverter.INSTANCE);			
			}
			
			return null;
		}
		
		return null;
	}
	
	private static ColumnType resolveColumnType(KeyInformation keyInfo, boolean isStatic) {
		if (isStatic) {
			return ColumnType.STATIC_COLUMN;
		}
		else if (keyInfo.isPartitionKey()) {
			return ColumnType.PARTITION_KEY;
		}
		else if (keyInfo.isClusteringColumn()) {
			return ColumnType.CLUSTERING_COLUMN;
		}
		else {
			return ColumnType.COLUMN;
		}
	}
	
	private static AbstractDataType resolveAbstractDataType(Method getter, Type genericJavaType, Class<?> javaType, ColumnType columnType) {
		
		DataType dataType = resolveDataType(getter, genericJavaType, javaType);
		if (dataType != null) {
			return new DTDataType(columnType, dataType);
		}
		else {
			
			IdentityName udtName = null;
			
			if (isUDTValue(javaType)) {
				UserTypeName userTypeName = getter.getDeclaredAnnotation(UserTypeName.class);
				if (userTypeName == null) {
					throw new CasserMappingException("absent UserTypeName annotation for " + getter);
				}
				udtName = new IdentityName(userTypeName.value(), userTypeName.forceQuote());
			}
			else {
			    udtName = MappingUtil.getUserDefinedTypeName(javaType, false);
			    
			    if (udtName == null) {
			    	throw new CasserMappingException("unknown property type for " + getter);
			    }
			    
			}
			
			return new UDTDataType(columnType, udtName);
		}
		
	}

	private static DataType resolveDataType(Method getter, Type genericJavaType, Class<?> javaType) {
		
		DataTypeName annotation = getter.getDeclaredAnnotation(DataTypeName.class);
		if (annotation != null && annotation.value() != null) {
			return qualifyAnnotatedType(getter, annotation);
		}
		
		if (isEnum(javaType)) {
			return DataType.text();
		}

		if (isMap(javaType)) {
			Type[] args = getTypeParameters(genericJavaType);
			ensureTypeArguments(getter, args.length, 2);

			return DataType.map(autodetectPrimitiveType(getter, args[0]),
					autodetectPrimitiveType(getter, args[1]));
		}

		if (isCollectionLike(javaType)) {
			Type[] args = getTypeParameters(genericJavaType);
			ensureTypeArguments(getter, args.length, 1);

			if (isSet(javaType)) {

				return DataType.set(autodetectPrimitiveType(getter, args[0]));

			} else if (isList(javaType)) {

				return DataType.list(autodetectPrimitiveType(getter, args[0]));

			}
		}

		return SimpleDataTypes.getDataTypeByJavaClass(javaType);
	}
	
	private static DataType qualifyAnnotatedType(Method getter, DataTypeName annotation) {
		DataType.Name type = annotation.value();
		if (type.isCollection()) {
			switch (type) {
			case MAP:
				ensureTypeArguments(getter, annotation.types().length, 2);
				return DataType.map(resolvePrimitiveType(getter, annotation.types()[0]),
						resolvePrimitiveType(getter, annotation.types()[1]));
			case LIST:
				ensureTypeArguments(getter, annotation.types().length, 1);
				return DataType.list(resolvePrimitiveType(getter, annotation.types()[0]));
			case SET:
				ensureTypeArguments(getter, annotation.types().length, 1);
				return DataType.set(resolvePrimitiveType(getter, annotation.types()[0]));
			default:
				throw new CasserMappingException("unknown collection DataType for property " + getter);
			}
		} else {
			DataType dataType = SimpleDataTypes.getDataTypeByName(type);
			
			if (dataType == null) {
				throw new CasserMappingException("unknown DataType for property " + getter);
			}
			
			return dataType;
		}
	}
	
	static DataType resolvePrimitiveType(Method getter, DataType.Name typeName) {
		DataType dataType = SimpleDataTypes.getDataTypeByName(typeName);
		if (dataType == null) {
			throw new CasserMappingException(
					"only primitive types are allowed inside collections for the property " + getter);
		}
		return dataType;
	}

	static DataType autodetectPrimitiveType(Method getter, Type type) {
		
		DataType dataType = null;
		
		if (type instanceof Class<?>) {
			Class<?> javaType = (Class<?>) type;
			dataType = SimpleDataTypes.getDataTypeByJavaClass(javaType);

			
			if (dataType == null) {
				IdentityName udtName = MappingUtil.getUserDefinedTypeName(javaType, false);
				
				if (udtName != null) {
					//return SchemaBuilder.frozen(udtName.getName());
				}
			}
			

		}
		
		if (dataType == null) {
			throw new CasserMappingException(
					"unknown type inside collections for the property " + getter);
		}
		
		return dataType;
	}
	
	static void ensureTypeArguments(Method getter, int args, int expected) {
		if (args != expected) {
			throw new CasserMappingException("expected " + expected + " of typed arguments for the property "
					+ getter);
		}
	}
	
	static boolean isMap(Class<?> javaType) {
		return Map.class.isAssignableFrom(javaType);
	}

	static boolean isSet(Class<?> javaType) {
		return Set.class.isAssignableFrom(javaType);
	}
	
	static boolean isList(Class<?> javaType) {
		return List.class.isAssignableFrom(javaType);
	}

	static boolean isEnum(Class<?> javaType) {
		return Enum.class.isAssignableFrom(javaType);
	}
	
	static boolean isCollectionLike(Class<?> javaType) {

		if (javaType.isArray() || Iterable.class.equals(javaType)) {
			return true;
		}

		return Collection.class.isAssignableFrom(javaType);
	}
	
	static boolean isUDTValue(Class<?> javaType) {
		return UDTValue.class.isAssignableFrom(javaType);
	}

	static boolean isTupleValue(Class<?> javaType) {
		return TupleValue.class.isAssignableFrom(javaType);
	}
	
	static Type[] getTypeParameters(Type genericJavaType) {
		
		if (genericJavaType instanceof ParameterizedType) {
		
			ParameterizedType type = (ParameterizedType) genericJavaType;
		
			return type.getActualTypeArguments();
		}
		
		return new Type[] {};
	}

}
