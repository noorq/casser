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
import com.noorq.casser.support.CasserMappingException;
import com.noorq.casser.support.Either;

public final class CasserMappingProperty implements CasserProperty {

	private final CasserEntity entity; 
	private final Method getter;
	
	private final String propertyName;
	private final IdentityName columnName;
	private final Optional<IdentityName> indexName;

	private final boolean isStatic;
	private final KeyInformation keyInfo;
	
	private final Class<?> javaType;
	private final Either<DataType, IdentityName> columnDataType;
		
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

		this.javaType = getter.getReturnType();
		this.columnDataType = resolveColumnDataType(getter, this.javaType);

	}
	
	@Override
	public CasserEntity getEntity() {
		return entity;
	}

	@Override
	public Class<?> getJavaType() {
		return javaType;
	}
	
	@Override
	public Either<DataType, IdentityName> getDataType() {
		return columnDataType;
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
		
		Either<DataType, IdentityName> columnType = getDataType();
		
		if (columnType.isRight()) {
			
			Class<Object> javaType = (Class<Object>) getJavaType();
			
			if (UDTValue.class.isAssignableFrom(javaType)) {
				return null;
			}
			
			return TypedConverter.create(
					UDTValue.class,
					javaType,
					new UDTValueToEntityConverter(javaType, repository));

		}
		else {
		
			Class<?> propertyType = getJavaType();
	
			if (Enum.class.isAssignableFrom(propertyType)) {
				return TypedConverter.create(
						String.class, 
						Enum.class, 
						new StringToEnumConverter(propertyType));
			}
	
			DataType dataType = columnType.getLeft();
	
			if (dataType.getName() == DataType.Name.TIMEUUID && propertyType == Date.class) {
				return TypedConverter.create(
						UUID.class, 
						Date.class, 
						TimeUUIDToDateConverter.INSTANCE);			
			}
	
			return null;
		}
	}
	
	@Override
	public Optional<Function<Object, Object>> getWriteConverter(SessionRepository repository) {
		
		if (writeConverter == null) {
			writeConverter = Optional.ofNullable(resolveWriteConverter(repository));
		}
		
		return writeConverter;
	}
	
	private Function<Object, Object> resolveWriteConverter(SessionRepository repository) {
	
		Either<DataType, IdentityName> columnType = getDataType();
		
		if (columnType.isRight()) {
			
			if (UDTValue.class.isAssignableFrom(javaType)) {
				return null;
			}
			
			Class<Object> javaType = (Class<Object>) getJavaType();
			
			UserType userType = repository.findUserType(columnType.getRight().getName());
			if (userType == null) {
				throw new CasserMappingException("UserType not found for " + columnType.getRight() + " with type " + javaType);
			}
			return TypedConverter.create(
					javaType, 
					UDTValue.class, 
					new EntityToUDTValueConverter(javaType, userType, repository));

		}
		else {
		
			Class<?> propertyType = getJavaType();
	
			if (Enum.class.isAssignableFrom(propertyType)) {
				
				return TypedConverter.create(
						Enum.class, 
						String.class, 
						EnumToStringConverter.INSTANCE);
				
			}
	
			DataType dataType = columnType.getLeft();
	
			if (dataType.getName() == DataType.Name.TIMEUUID && propertyType == Date.class) {
				
				return TypedConverter.create(
						Date.class, 
						UUID.class, 
						DateToTimeUUIDConverter.INSTANCE);			
			}
			
			return null;
		}
	}
	
	private static Either<DataType, IdentityName> resolveColumnDataType(Method getter, Class<?> javaType) {
		
		DataType dataType = resolveDataType(getter, javaType);
		if (dataType != null) {
			return Either.left(dataType);
		}
		else {
			
			IdentityName udtName = null;
			
			if (UDTValue.class.isAssignableFrom(javaType)) {
				UserTypeName userTypeName = getter.getDeclaredAnnotation(UserTypeName.class);
				if (userTypeName == null) {
					throw new CasserMappingException("absent UserTypeName annotation for " + getter);
				}
				udtName = new IdentityName(userTypeName.value(), userTypeName.forceQuote());
			}
			else {
			    udtName = MappingUtil.getUserDefinedTypeName(javaType, false);
			}
			
			return Either.right(udtName);
		}
		
	}

	private static DataType resolveDataType(Method getter, Class<?> javaType) {
		
		DataTypeName annotation = getter.getDeclaredAnnotation(DataTypeName.class);
		if (annotation != null && annotation.value() != null) {
			return qualifyAnnotatedType(getter, annotation);
		}
		
		if (Enum.class.isAssignableFrom(javaType)) {
			return DataType.text();
		}

		if (isMap(javaType)) {
			Type[] args = getTypeParameters(javaType);
			ensureTypeArguments(getter, args.length, 2);

			return DataType.map(autodetectPrimitiveType(getter, args[0]),
					autodetectPrimitiveType(getter, args[1]));
		}

		if (isCollectionLike(javaType)) {
			Type[] args = getTypeParameters(javaType);
			ensureTypeArguments(getter, args.length, 1);

			if (Set.class.isAssignableFrom(javaType)) {

				return DataType.set(autodetectPrimitiveType(getter, args[0]));

			} else if (List.class.isAssignableFrom(javaType)) {

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

	static DataType autodetectPrimitiveType(Method getter, Type javaType) {
		DataType dataType = SimpleDataTypes.getDataTypeByJavaClass(javaType);
		if (dataType == null) {
			throw new CasserMappingException(
					"only primitive types are allowed inside collections for the property " + getter);
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
	
	static boolean isCollectionLike(Class<?> javaType) {

		Class<?> rawType = javaType;

		if (rawType.isArray() || Iterable.class.equals(rawType)) {
			return true;
		}

		return Collection.class.isAssignableFrom(rawType);
	}
	
	static Type[] getTypeParameters(Class<?> javaTypeAsClass) {
		
		Type javaType = (Type) javaTypeAsClass;
		
		if (javaType instanceof ParameterizedType) {
		
			ParameterizedType type = (ParameterizedType) javaType;
		
			return type.getActualTypeArguments();
		}
		
		return new Type[] {};
	}

}
