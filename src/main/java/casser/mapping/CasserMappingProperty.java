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

import casser.core.Casser;
import casser.mapping.convert.DateToTimeUUIDConverter;
import casser.mapping.convert.EnumToStringConverter;
import casser.mapping.convert.StringToEnumConverter;
import casser.mapping.convert.TimeUUIDToDateConverter;
import casser.mapping.convert.TypedConverter;
import casser.support.CasserMappingException;

import com.datastax.driver.core.DataType;

public class CasserMappingProperty<E> implements CasserProperty<E> {

	private final CasserMappingEntity<E> entity; 
	
	private final Method getterMethod;
	private Method setterMethod;
	
	private final String propertyName;
	
	private boolean keyInfo = false;
	private boolean isPartitionKey = false;
	private boolean isClusteringColumn = false;
	private int ordinal = 0;
	private Ordering ordering = Ordering.ASCENDING;
	
	private Class<?> javaType = null;
	
	private boolean typeInfo = false;
	private DataType dataType = null;
	
	private boolean staticInfo = false;
	private boolean isStatic = false;
	
	private String columnName;
	
	private Optional<Function<Object, Object>> readConverter = null;
	private Optional<Function<Object, Object>> writeConverter = null;
	
	public CasserMappingProperty(CasserMappingEntity<E> entity, Method getter) {
		this.entity = entity;
		this.getterMethod = getter;
		this.propertyName = getPropertyName(getter);
	}
	
	@Override
	public CasserMappingEntity<E> getEntity() {
		return entity;
	}
	
	private void ensureKeyInfo() {
		if (!keyInfo) {
			PartitionKey partitionKey = getterMethod.getDeclaredAnnotation(PartitionKey.class);
			
			if (partitionKey != null) {
				isPartitionKey = true;
				ordinal = partitionKey.ordinal();
			}
			
			ClusteringColumn clusteringColumnn = getterMethod.getDeclaredAnnotation(ClusteringColumn.class);
			
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

    @Override
	public Class<?> getJavaType() {
		if (javaType == null) {
			javaType = getterMethod.getReturnType();
		}
		return javaType;
	}
	
	@Override
	public DataType getDataType() {

		if (!typeInfo) {
			dataType = resolveDataType();
			typeInfo = true;
		}
		
		return dataType;
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
	public Ordering getOrdering() {
		ensureKeyInfo();
		return ordering;
	}
	
	@Override
	public boolean isStatic() {
		
		if (!staticInfo) {
			
			Column column = getterMethod.getDeclaredAnnotation(Column.class);
			if (column != null) {
				isStatic = column.isStatic();
			}
			
			staticInfo = true;
			
		}

		return isStatic;
	}

	@Override
	public String getColumnName() {
		
		if (columnName == null) {
			
			Column column = getterMethod.getDeclaredAnnotation(Column.class);
			if (column != null) {
				columnName = column.value();
			}

			PartitionKey partitionKey = getterMethod.getDeclaredAnnotation(PartitionKey.class);
			if (partitionKey != null) {
				
				if (columnName != null) {
					throw new CasserMappingException("property can be annotated only by single column type " + getPropertyName() + " in " + this.entity.getMappingInterface());
				}
				
				columnName = partitionKey.value();
			}
			
			ClusteringColumn clusteringColumn = getterMethod.getDeclaredAnnotation(ClusteringColumn.class);
			if (clusteringColumn != null) {
				
				if (columnName != null) {
					throw new CasserMappingException("property can be annotated only by single column type " + getPropertyName() + " in " + this.entity.getMappingInterface());
				}
				
				columnName = clusteringColumn.value();
			}
			
			if (columnName == null || columnName.isEmpty()) {
				columnName = getDefaultColumnName();
			}
			
		}
		
		return columnName;
	}
	
	private String getDefaultColumnName() {
		return Casser.settings().getPropertyToColumnConverter().apply(propertyName);
	}

	public void setSetterMethod(Method setterMethod) {
		this.setterMethod = setterMethod;
	}

	public String getPropertyName() {
		return propertyName;
	}

	public Method getGetterMethod() {
		return getterMethod;
	}

	public Method getSetterMethod() {
		return setterMethod;
	}
	
	@Override
	public Optional<Function<Object, Object>> getReadConverter() {

		if (readConverter == null) {
			readConverter = Optional.ofNullable(resolveReadConverter());
		}
		
		return readConverter;
	}

	private Function<Object, Object> resolveReadConverter() {
		
		Class<?> propertyType = getJavaType();

		if (Enum.class.isAssignableFrom(propertyType)) {
			return TypedConverter.create(
					String.class, 
					Enum.class, 
					new StringToEnumConverter(propertyType));
		}

		DataType dataType = getDataType();

		if (dataType.getName() == DataType.Name.TIMEUUID && propertyType == Date.class) {
			return TypedConverter.create(
					UUID.class, 
					Date.class, 
					TimeUUIDToDateConverter.INSTANCE);			
		}

		return null;
	}
	
	@Override
	public Optional<Function<Object, Object>> getWriteConverter() {
		
		if (writeConverter == null) {
			writeConverter = Optional.ofNullable(resolveWriteConverter());
		}
		
		return writeConverter;
	}
	
	private Function<Object, Object> resolveWriteConverter() {
	
		Class<?> propertyType = getJavaType();

		if (Enum.class.isAssignableFrom(propertyType)) {
			
			return TypedConverter.create(
					Enum.class, 
					String.class, 
					EnumToStringConverter.INSTANCE);
			
		}

		DataType dataType = getDataType();

		if (dataType.getName() == DataType.Name.TIMEUUID && propertyType == Date.class) {
			
			return TypedConverter.create(
					Date.class, 
					UUID.class, 
					DateToTimeUUIDConverter.INSTANCE);			
		}
		return null;
	}
	
	public static String getPropertyName(Method m) {
		
		String propertyName = Casser.settings().getMethodNameToPropertyConverter().apply(m.getName());
		return propertyName;
	}
	
	private DataType resolveDataType() {
		
		Class<?> propertyType = getJavaType();

		Qualify annotation = getterMethod.getDeclaredAnnotation(Qualify.class);
		if (annotation != null && annotation.type() != null) {
			return qualifyAnnotatedType(annotation);
		}

		if (Enum.class.isAssignableFrom(propertyType)) {
			return DataType.text();
		}

		if (isMap()) {
			Type[] args = getTypeArguments();
			ensureTypeArguments(args.length, 2);

			return DataType.map(autodetectPrimitiveType(args[0]),
					autodetectPrimitiveType(args[1]));
		}

		if (isCollectionLike()) {
			Type[] args = getTypeArguments();
			ensureTypeArguments(args.length, 1);

			if (Set.class.isAssignableFrom(propertyType)) {

				return DataType.set(autodetectPrimitiveType(args[0]));

			} else if (List.class.isAssignableFrom(propertyType)) {

				return DataType.list(autodetectPrimitiveType(args[0]));

			}
		}

		DataType dataType = SimpleDataTypes.getDataTypeByJavaClass(propertyType);
		if (dataType == null) {
			throw new CasserMappingException(
					"only primitive types and Set,List,Map collections are allowed, unknown type for property '" + this.getPropertyName()
							+ "' type is '" + this.getJavaType() + "' in the entity " + this.entity.getMappingInterface());
		}

		return dataType;
	}
	
	private DataType qualifyAnnotatedType(Qualify annotation) {
		DataType.Name type = annotation.type();
		if (type.isCollection()) {
			switch (type) {
			case MAP:
				ensureTypeArguments(annotation.typeArguments().length, 2);
				return DataType.map(resolvePrimitiveType(annotation.typeArguments()[0]),
						resolvePrimitiveType(annotation.typeArguments()[1]));
			case LIST:
				ensureTypeArguments(annotation.typeArguments().length, 1);
				return DataType.list(resolvePrimitiveType(annotation.typeArguments()[0]));
			case SET:
				ensureTypeArguments(annotation.typeArguments().length, 1);
				return DataType.set(resolvePrimitiveType(annotation.typeArguments()[0]));
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
	
	Type[] getTypeArguments() {
		
		Type javaType = (Type) getJavaType();
		
		if (javaType instanceof ParameterizedType) {
		
			ParameterizedType type = (ParameterizedType) javaType;
		
			return type.getActualTypeArguments();
		}
		
		return new Type[] {};
	}

}
