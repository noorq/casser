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
import java.util.stream.Collectors;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.noorq.casser.core.Casser;
import com.noorq.casser.core.SessionRepository;
import com.noorq.casser.mapping.annotation.DataTypeName;
import com.noorq.casser.mapping.annotation.type.UDT;
import com.noorq.casser.mapping.convert.DateToTimeUUIDConverter;
import com.noorq.casser.mapping.convert.EntityToTupleValueConverter;
import com.noorq.casser.mapping.convert.EntityToUDTValueConverter;
import com.noorq.casser.mapping.convert.EnumToStringConverter;
import com.noorq.casser.mapping.convert.StringToEnumConverter;
import com.noorq.casser.mapping.convert.TimeUUIDToDateConverter;
import com.noorq.casser.mapping.convert.TupleValueToEntityConverter;
import com.noorq.casser.mapping.convert.TypedConverter;
import com.noorq.casser.mapping.convert.UDTValueToEntityConverter;
import com.noorq.casser.mapping.type.AbstractDataType;
import com.noorq.casser.mapping.type.DTDataType;
import com.noorq.casser.mapping.type.DTKeyUTDValueMapDataType;
import com.noorq.casser.mapping.type.UDTDataType;
import com.noorq.casser.mapping.type.UDTKeyDTValueMapDataType;
import com.noorq.casser.mapping.type.UDTKeyUDTValueMapDataType;
import com.noorq.casser.mapping.type.UDTListDataType;
import com.noorq.casser.mapping.type.UDTSetDataType;
import com.noorq.casser.support.CasserMappingException;
import com.noorq.casser.support.Either;

public final class CasserMappingProperty implements CasserProperty {

	private final CasserEntity entity; 
	private final Method getter;
	
	private final String propertyName;
	private final Optional<IdentityName> indexName;

	private final ColumnInformation columnInfo;
	
	private final Type genericJavaType;
	private final Class<?> javaType;
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

		this.dataType = resolveDataType(getter, this.genericJavaType, this.javaType, this.columnInfo.getColumnType());

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
		
			DataType dataType = ((DTDataType) abstractDataType).getDataType();
			Class<Object> javaType = (Class<Object>) getJavaType();
			
			if (dataType.getName() == DataType.Name.TUPLE) {
				
				return TypedConverter.create(
						TupleValue.class,
						javaType,
						new TupleValueToEntityConverter(javaType, repository));
			}
			
			if (Enum.class.isAssignableFrom(javaType)) {
				return TypedConverter.create(
						String.class, 
						Enum.class, 
						new StringToEnumConverter(javaType));
			}
	
			if (dataType.getName() == DataType.Name.TIMEUUID && Date.class.isAssignableFrom(javaType)) {
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
			
			DataType dataType = ((DTDataType) abstractDataType).getDataType();
			Class<Object> javaType = (Class<Object>) getJavaType();
	
			if (dataType.getName() == DataType.Name.TUPLE) {
				
				return TypedConverter.create(
						javaType, 
						TupleValue.class, 
						new EntityToTupleValueConverter(javaType, (TupleType) dataType, repository));
			}
			
			if (Enum.class.isAssignableFrom(javaType)) {
				
				return TypedConverter.create(
						Enum.class, 
						String.class, 
						EnumToStringConverter.INSTANCE);
				
			}
	
			if (dataType.getName() == DataType.Name.TIMEUUID && Date.class.isAssignableFrom(javaType)) {
				
				return TypedConverter.create(
						Date.class, 
						UUID.class, 
						DateToTimeUUIDConverter.INSTANCE);			
			}
			
			return null;
		}
		
		return null;
	}
	
	private static AbstractDataType resolveDataType(Method getter, Type genericJavaType, Class<?> javaType, ColumnType columnType) {
		
		DataTypeName annotation = getter.getDeclaredAnnotation(DataTypeName.class);
		if (annotation != null && annotation.value() != null) {
			return resolveDataTypeByAnnotation(getter, annotation, columnType);
		}
		
		if (isEnum(javaType)) {
			return new DTDataType(columnType, DataType.text());
		}

		if (isMap(javaType)) {
			
			Type[] args = getTypeParameters(genericJavaType);
			ensureTypeArguments(getter, args.length, 2);

			Either<DataType, IdentityName> key = autodetectParameterType(getter, args[0]);
			Either<DataType, IdentityName> value = autodetectParameterType(getter, args[1]);
			
			if (key.isLeft()) {
				
				if (value.isLeft()) {
					return new DTDataType(columnType, DataType.map(key.getLeft(), value.getLeft()));
				}
				else {
					return new DTKeyUTDValueMapDataType(columnType, 
							key.getLeft(), 
							value.getRight(),
							(Class<?>) args[1]);
				}
			}
			else {
				
				if (value.isLeft()) {
					return new UDTKeyDTValueMapDataType(columnType, 
							key.getRight(), 
							(Class<?>) args[0],
							value.getLeft());
				}
				else {
					return new UDTKeyUDTValueMapDataType(columnType, 
							key.getRight(), 
							(Class<?>) args[0],
							value.getRight(),
							(Class<?>) args[1]);
				}
				
			}

		}

		if (isCollectionLike(javaType)) {
			
			Type[] args = getTypeParameters(genericJavaType);
			ensureTypeArguments(getter, args.length, 1);

			if (isSet(javaType)) {
				
				Either<DataType, IdentityName> parameterType = autodetectParameterType(getter, args[0]);

				if (parameterType.isLeft()) {
					return new DTDataType(columnType, DataType.set(parameterType.getLeft()));
				}
				else {
					return new UDTSetDataType(columnType, 
							parameterType.getRight(),
							(Class<?>) args[0]);
				}
	
			} else if (isList(javaType)) {

				Either<DataType, IdentityName> parameterType = autodetectParameterType(getter, args[0]);

				if (parameterType.isLeft()) {
					return new DTDataType(columnType, DataType.list(parameterType.getLeft()));
				}
				else {
					return new UDTListDataType(columnType, 
							parameterType.getRight(),
							(Class<?>) args[0]);
				}

			}
		}

		DataType dataType = SimpleDataTypes.getDataTypeByJavaClass(javaType);
		if (dataType != null) {
			return new DTDataType(columnType, dataType);
		}
		
	    if (MappingUtil.isTuple(javaType)) {
	    	
	    	CasserEntity tupleEntity = Casser.entity(javaType);
	    	
	    	List<DataType> tupleTypes = tupleEntity.getOrderedProperties().stream()
		    	.map(p -> p.getDataType())
		    	.filter(d -> d instanceof DTDataType)
		    	.map(d -> (DTDataType) d)
		    	.map(d -> d.getDataType())
		    	.collect(Collectors.toList());
	    	
	    	if (tupleTypes.size() < tupleEntity.getOrderedProperties().size()) {
	    		
	    		List<IdentityName> wrongColumns = tupleEntity.getOrderedProperties().stream()
	    				.filter(p -> !(p.getDataType() instanceof DTDataType))
	    				.map(p -> p.getColumnName())
	    				.collect(Collectors.toList());
	    		
	    		throw new CasserMappingException("non simple types in tuple " + tupleEntity.getMappingInterface() + " in columns: " + wrongColumns);
	    	}
	    	
	    	TupleType tupleType = TupleType.of(tupleTypes.toArray(new DataType[tupleTypes.size()]));
	    	
	    	return new DTDataType(columnType, tupleType);
	    }
		
		IdentityName udtName = null;
		
		if (isUDTValue(javaType)) {
			
			UDT userTypeName = getter.getDeclaredAnnotation(UDT.class);
			if (userTypeName == null) {
				throw new CasserMappingException("absent UserTypeName annotation for " + getter);
			}
			
			udtName = new IdentityName(userTypeName.value(), userTypeName.forceQuote());
		}
		else {
		    udtName = MappingUtil.getUserDefinedTypeName(javaType, false);
		}
		
		if (udtName != null) {
			return new UDTDataType(columnType, udtName, javaType);
		}
		
		throw new CasserMappingException("unknown type " + javaType + " in " + getter);
		
	}
	
	private static AbstractDataType resolveDataTypeByAnnotation(Method getter, DataTypeName annotation, ColumnType columnType) {
		
		DataType.Name type = annotation.value();
		
		if (type.isCollection()) {
			
			switch (type) {
			case MAP:
				ensureTypeArguments(getter, annotation.types().length, 2);
				
				return new DTDataType(columnType, 
						DataType.map(resolveSimpleType(getter, annotation.types()[0]),
						resolveSimpleType(getter, annotation.types()[1])));
				
			case LIST:
				ensureTypeArguments(getter, annotation.types().length, 1);
				
				return new DTDataType(columnType, 
						DataType.list(resolveSimpleType(getter, annotation.types()[0])));
				
			case SET:
				ensureTypeArguments(getter, annotation.types().length, 1);
				
				return new DTDataType(columnType, 
						DataType.set(resolveSimpleType(getter, annotation.types()[0])));
				
			default:
				throw new CasserMappingException("unknown collection DataType for property " + getter);
			}
			
		} 
		
		DataType dataType = SimpleDataTypes.getDataTypeByName(type);
			
		if (dataType != null) {
			return new DTDataType(columnType, dataType);
		}

		throw new CasserMappingException("unknown DataType for property " + getter);
		
	}
	
	static DataType resolveSimpleType(Method getter, DataType.Name typeName) {
		DataType dataType = SimpleDataTypes.getDataTypeByName(typeName);
		if (dataType == null) {
			throw new CasserMappingException(
					"only primitive types are allowed inside collections for the property " + getter);
		}
		return dataType;
	}

	static Either<DataType, IdentityName> autodetectParameterType(Method getter, Type type) {
		
		DataType dataType = null;
		
		if (type instanceof Class<?>) {
			
			Class<?> javaType = (Class<?>) type;
			dataType = SimpleDataTypes.getDataTypeByJavaClass(javaType);

			if (dataType != null) {
				return Either.left(dataType);
			}
			
			IdentityName udtName = MappingUtil.getUserDefinedTypeName(javaType, false);
				
			if (udtName != null) {
				return Either.right(udtName);
			}

		}

		throw new CasserMappingException(
				"unknown parameter type " + type + " in the collection for the property " + getter);
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
