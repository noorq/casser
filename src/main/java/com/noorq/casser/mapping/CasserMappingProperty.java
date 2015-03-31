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
import com.noorq.casser.mapping.convert.DateToTimeUUIDConverter;
import com.noorq.casser.mapping.convert.EntityToUDTValueConverter;
import com.noorq.casser.mapping.convert.EnumToStringConverter;
import com.noorq.casser.mapping.convert.StringToEnumConverter;
import com.noorq.casser.mapping.convert.TimeUUIDToDateConverter;
import com.noorq.casser.mapping.convert.TypedConverter;
import com.noorq.casser.mapping.convert.UDTValueToEntityConverter;
import com.noorq.casser.support.CasserMappingException;
import com.noorq.casser.support.Either;

public class CasserMappingProperty implements CasserProperty {

	private final CasserMappingEntity entity; 
	private final Method getter;
	
	private Optional<String> propertyName = Optional.empty();
	private Optional<IdentityName> columnName = Optional.empty();
	
	private Optional<IdentityName> indexName = null;
	
	private boolean keyInfo = false;
	private boolean isPartitionKey = false;
	private boolean isClusteringColumn = false;
	private int ordinal = 0;
	private OrderingDirection ordering = OrderingDirection.ASC;
	
	private Optional<Class<?>> javaType = Optional.empty();
	private Optional<Either<DataType, IdentityName>> columnType = Optional.empty();
	
	private Optional<Boolean> isStatic = Optional.empty();
	
	private Optional<Function<Object, Object>> readConverter = null;
	private Optional<Function<Object, Object>> writeConverter = null;
	
	public CasserMappingProperty(CasserMappingEntity entity, Method getter) {
		this.entity = entity;
		this.getter = getter;
	}
	
	@Override
	public CasserMappingEntity getEntity() {
		return entity;
	}

    @Override
	public Class<?> getJavaType() {
		if (!javaType.isPresent()) {
			javaType = Optional.of(getter.getReturnType());
		}
		return javaType.get();
	}
	
	@Override
	public Either<DataType, IdentityName> getColumnType() {
		if (!columnType.isPresent()) {
			columnType = Optional.of(resolveColumnType());
		}
		return columnType.get();
	}

	@Override
	public boolean isPartitionKey() {
		ensureKeyInfo();
		return isPartitionKey;
	}

	@Override
	public boolean isClusteringColumn() {
		ensureKeyInfo();
		return isClusteringColumn;
	}

	@Override
	public int getOrdinal() {
		ensureKeyInfo();
		return ordinal;
	}

	@Override
	public OrderingDirection getOrdering() {
		ensureKeyInfo();
		return ordering;
	}
	
	@Override
	public boolean isStatic() {
		
		if (!isStatic.isPresent()) {
			
			Column column = getter.getDeclaredAnnotation(Column.class);
			if (column != null) {
				isStatic = Optional.of(column.isStatic());
			}
			else {
				isStatic = Optional.of(false);
			}

		}

		return isStatic.get().booleanValue();
	}

	@Override
	public IdentityName getColumnName() {
		
		if (!columnName.isPresent()) {
			columnName = Optional.of(MappingUtil.getColumnName(getter));
		}
		
		return columnName.get();
	}

	@Override
	public Optional<IdentityName> getIndexName() {
		
		if (indexName == null) {
			indexName = MappingUtil.getIndexName(getter);
		}
		
		return indexName;
	}
	
	@Override
	public String getPropertyName() {
		
		if (!propertyName.isPresent()) {
			propertyName = Optional.of(MappingUtil.getPropertyName(getter));
		}
		
		return propertyName.get();
	}

	public Method getGetterMethod() {
		return getter;
	}
	
	@Override
	public Optional<Function<Object, Object>> getReadConverter(CasserMappingRepository repository) {

		if (readConverter == null) {
			readConverter = Optional.ofNullable(resolveReadConverter(repository));
		}
		
		return readConverter;
	}

	private Function<Object, Object> resolveReadConverter(CasserMappingRepository repository) {
		
		Either<DataType, IdentityName> columnType = getColumnType();
		
		if (columnType.isRight()) {
			
			Class<Object> javaType = (Class<Object>) getJavaType();
			
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
	public Optional<Function<Object, Object>> getWriteConverter(CasserMappingRepository repository) {
		
		if (writeConverter == null) {
			writeConverter = Optional.ofNullable(resolveWriteConverter(repository));
		}
		
		return writeConverter;
	}
	
	private Function<Object, Object> resolveWriteConverter(CasserMappingRepository repository) {
	
		Either<DataType, IdentityName> columnType = getColumnType();
		
		if (columnType.isRight()) {
			
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
	
	private Either<DataType, IdentityName> resolveColumnType() {
		
		DataType dataType = resolveDataType();
		if (dataType != null) {
			return Either.left(dataType);
		}
		else {
			Class<?> propertyType = getJavaType();
			IdentityName udtName = MappingUtil.getUserDefinedTypeName(propertyType, false);
			return Either.right(udtName);
		}
		
	}

	private DataType resolveDataType() {
		
		Class<?> propertyType = getJavaType();

		DataTypeName annotation = getter.getDeclaredAnnotation(DataTypeName.class);
		if (annotation != null && annotation.value() != null) {
			return qualifyAnnotatedType(annotation);
		}

		if (Enum.class.isAssignableFrom(propertyType)) {
			return DataType.text();
		}

		if (isMap()) {
			Type[] args = getTypeParameters();
			ensureTypeArguments(args.length, 2);

			return DataType.map(autodetectPrimitiveType(args[0]),
					autodetectPrimitiveType(args[1]));
		}

		if (isCollectionLike()) {
			Type[] args = getTypeParameters();
			ensureTypeArguments(args.length, 1);

			if (Set.class.isAssignableFrom(propertyType)) {

				return DataType.set(autodetectPrimitiveType(args[0]));

			} else if (List.class.isAssignableFrom(propertyType)) {

				return DataType.list(autodetectPrimitiveType(args[0]));

			}
		}

		return SimpleDataTypes.getDataTypeByJavaClass(propertyType);
	}
	
	private void ensureKeyInfo() {
		if (!keyInfo) {
			PartitionKey partitionKey = getter.getDeclaredAnnotation(PartitionKey.class);
			
			if (partitionKey != null) {
				isPartitionKey = true;
				ordinal = partitionKey.ordinal();
			}
			
			ClusteringColumn clusteringColumnn = getter.getDeclaredAnnotation(ClusteringColumn.class);
			
			if (clusteringColumnn != null) {
				
				if (isPartitionKey) {
					throw new CasserMappingException("property can be annotated only by single column type " + getPropertyName() + " in " + this.entity.getMappingInterface());
				}
				
				isClusteringColumn = true;
				ordinal = clusteringColumnn.ordinal();
				ordering = clusteringColumnn.ordering();
			}
			
			keyInfo = true;
		}
	}
	
	private DataType qualifyAnnotatedType(DataTypeName annotation) {
		DataType.Name type = annotation.value();
		if (type.isCollection()) {
			switch (type) {
			case MAP:
				ensureTypeArguments(annotation.typeParameters().length, 2);
				return DataType.map(resolvePrimitiveType(annotation.typeParameters()[0]),
						resolvePrimitiveType(annotation.typeParameters()[1]));
			case LIST:
				ensureTypeArguments(annotation.typeParameters().length, 1);
				return DataType.list(resolvePrimitiveType(annotation.typeParameters()[0]));
			case SET:
				ensureTypeArguments(annotation.typeParameters().length, 1);
				return DataType.set(resolvePrimitiveType(annotation.typeParameters()[0]));
			default:
				throw new CasserMappingException("unknown collection DataType for property '" + this.getPropertyName()
						+ "' type is '" + this.getJavaType() + "' in the entity " + this.entity.getMappingInterface());
			}
		} else {
			DataType dataType = SimpleDataTypes.getDataTypeByName(type);
			
			if (dataType == null) {
				throw new CasserMappingException("unknown DataType for property '" + this.getPropertyName()
						+ "' type is '" + this.getJavaType() + "' in the entity " + this.entity.getMappingInterface());
			}
			
			return dataType;
		}
	}
	
	DataType resolvePrimitiveType(DataType.Name typeName) {
		DataType dataType = SimpleDataTypes.getDataTypeByName(typeName);
		if (dataType == null) {
			throw new CasserMappingException(
					"only primitive types are allowed inside collections for the property  '" + this.getPropertyName() + "' type is '"
							+ this.getJavaType() + "' in the entity " + this.entity.getMappingInterface());
		}
		return dataType;
	}

	DataType autodetectPrimitiveType(Type javaType) {
		DataType dataType = SimpleDataTypes.getDataTypeByJavaClass(javaType);
		if (dataType == null) {
			throw new CasserMappingException(
					"only primitive types are allowed inside collections for the property  '" + this.getPropertyName() + "' type is '"
							+ this.getJavaType() + "' in the entity " + this.entity.getMappingInterface());
		}
		return dataType;
	}
	
	void ensureTypeArguments(int args, int expected) {
		if (args != expected) {
			throw new CasserMappingException("expected " + expected + " of typed arguments for the property  '"
					+ this.getPropertyName() + "' type is '" + this.getJavaType() + "' in the entity " + this.entity.getMappingInterface());
		}
	}
	
	boolean isMap() {
		return Map.class.isAssignableFrom(getJavaType());
	}
	
	boolean isCollectionLike() {

		Class<?> rawType = getJavaType();

		if (rawType.isArray() || Iterable.class.equals(rawType)) {
			return true;
		}

		return Collection.class.isAssignableFrom(rawType);
	}
	
	Type[] getTypeParameters() {
		
		Type javaType = (Type) getJavaType();
		
		if (javaType instanceof ParameterizedType) {
		
			ParameterizedType type = (ParameterizedType) javaType;
		
			return type.getActualTypeArguments();
		}
		
		return new Type[] {};
	}

}
