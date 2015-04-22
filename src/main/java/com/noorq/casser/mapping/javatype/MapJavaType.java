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
package com.noorq.casser.mapping.javatype;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.noorq.casser.core.SessionRepository;
import com.noorq.casser.mapping.ColumnType;
import com.noorq.casser.mapping.IdentityName;
import com.noorq.casser.mapping.annotation.Types;
import com.noorq.casser.mapping.convert.MapToUDTKeyMapConverter;
import com.noorq.casser.mapping.convert.MapToUDTMapConverter;
import com.noorq.casser.mapping.convert.MapToUDTValueMapConverter;
import com.noorq.casser.mapping.convert.UDTKeyMapToMapConverter;
import com.noorq.casser.mapping.convert.UDTMapToMapConverter;
import com.noorq.casser.mapping.convert.UDTValueMapToMapConverter;
import com.noorq.casser.mapping.type.AbstractDataType;
import com.noorq.casser.mapping.type.DTDataType;
import com.noorq.casser.mapping.type.UDTKeyMapDataType;
import com.noorq.casser.mapping.type.UDTMapDataType;
import com.noorq.casser.mapping.type.UDTValueMapDataType;
import com.noorq.casser.support.CasserMappingException;
import com.noorq.casser.support.Either;

public final class MapJavaType extends AbstractJavaType {

	@Override
	public Class<?> getJavaClass() {
		return Map.class;
	}

	@Override
	public AbstractDataType resolveDataType(Method getter, Type genericJavaType, ColumnType columnType) {

		Types.Map cmap = getter.getDeclaredAnnotation(Types.Map.class);
		if (cmap != null) {
			return new DTDataType(columnType, 
					DataType.map(
							resolveSimpleType(getter, cmap.key()),
							resolveSimpleType(getter, cmap.value())));
		}

		Types.UDTKeyMap udtKeyMap = getter.getDeclaredAnnotation(Types.UDTKeyMap.class);
		if (udtKeyMap != null) {
			return new UDTKeyMapDataType(columnType, 
					resolveUDT(udtKeyMap.key()),
					UDTValue.class,
					resolveSimpleType(getter, udtKeyMap.value()));
		}

		Types.UDTValueMap udtValueMap = getter.getDeclaredAnnotation(Types.UDTValueMap.class);
		if (udtValueMap != null) {
			return new UDTValueMapDataType(columnType, 
					resolveSimpleType(getter, udtValueMap.key()),
					resolveUDT(udtValueMap.value()),
					UDTValue.class);
		}
		
		Types.UDTMap udtMap = getter.getDeclaredAnnotation(Types.UDTMap.class);
		if (udtMap != null) {
			return new UDTMapDataType(columnType, 
					resolveUDT(udtMap.key()),
					UDTValue.class,
					resolveUDT(udtMap.value()),
					UDTValue.class);
		}

		Type[] args = getTypeParameters(genericJavaType);
		ensureTypeArguments(getter, args.length, 2);
		
		Either<DataType, IdentityName> key = autodetectParameterType(getter, args[0]);
		Either<DataType, IdentityName> value = autodetectParameterType(getter, args[1]);
		
		if (key.isLeft()) {
			
			if (value.isLeft()) {
				return new DTDataType(columnType, 
						DataType.map(key.getLeft(), value.getLeft()));
			}
			else {
				return new UDTValueMapDataType(columnType, 
						key.getLeft(), 
						value.getRight(),
						(Class<?>) args[1]);
			}
		}
		else {
			
			if (value.isLeft()) {
				return new UDTKeyMapDataType(columnType, 
						key.getRight(), 
						(Class<?>) args[0],
						value.getLeft());
			}
			else {
				return new UDTMapDataType(columnType, 
						key.getRight(), 
						(Class<?>) args[0],
						value.getRight(),
						(Class<?>) args[1]);
			}
			
		}
	}
	

	@Override
	public Optional<Function<Object, Object>> resolveReadConverter(
			AbstractDataType abstractDataType, SessionRepository repository) {
		
		if (abstractDataType instanceof UDTKeyMapDataType) {
			
			UDTKeyMapDataType dt = (UDTKeyMapDataType) abstractDataType;
			
			Class<Object> javaClass = (Class<Object>) dt.getUdtKeyClass();
			
			if (UDTValue.class.isAssignableFrom(javaClass)) {
				return Optional.empty();
			}
			
			return Optional.of(new UDTKeyMapToMapConverter(javaClass, repository));
			
		}
		
		else if (abstractDataType instanceof UDTValueMapDataType) {
			
			UDTValueMapDataType dt = (UDTValueMapDataType) abstractDataType;
			
			Class<Object> javaClass = (Class<Object>) dt.getUdtValueClass();
			
			if (UDTValue.class.isAssignableFrom(javaClass)) {
				return Optional.empty();
			}
			
			return Optional.of(new UDTValueMapToMapConverter(javaClass, repository));
			
		}
		
		else if (abstractDataType instanceof UDTMapDataType) {
			
			UDTMapDataType dt = (UDTMapDataType) abstractDataType;
			
			Class<Object> keyClass = (Class<Object>) dt.getUdtKeyClass();
			Class<Object> valueClass = (Class<Object>) dt.getUdtValueClass();
			
			if (UDTValue.class.isAssignableFrom(keyClass) && UDTValue.class.isAssignableFrom(valueClass)) {
				return Optional.empty();
			}
			
			return Optional.of(new UDTMapToMapConverter(keyClass, valueClass, repository));
			
		}
		
		return Optional.empty();
	}

	@Override
	public Optional<Function<Object, Object>> resolveWriteConverter(
			AbstractDataType abstractDataType, SessionRepository repository) {
		
		if (abstractDataType instanceof UDTKeyMapDataType) {
			
			UDTKeyMapDataType dt = (UDTKeyMapDataType) abstractDataType;
			
			Class<Object> javaClass = (Class<Object>) dt.getUdtKeyClass();
			
			if (UDTValue.class.isAssignableFrom(javaClass)) {
				return Optional.empty();
			}

			UserType userType = repository.findUserType(dt.getUdtKeyName().getName());
			if (userType == null) {
				throw new CasserMappingException("UserType not found for " + dt.getUdtKeyName() + " with type " + javaClass);
			}
			
			return Optional.of(new MapToUDTKeyMapConverter(javaClass, userType, repository));
			
		}
		
		else if (abstractDataType instanceof UDTValueMapDataType) {
			
			UDTValueMapDataType dt = (UDTValueMapDataType) abstractDataType;
			
			Class<Object> javaClass = (Class<Object>) dt.getUdtValueClass();
			
			if (UDTValue.class.isAssignableFrom(javaClass)) {
				return Optional.empty();
			}

			UserType userType = repository.findUserType(dt.getUdtValueName().getName());
			if (userType == null) {
				throw new CasserMappingException("UserType not found for " + dt.getUdtValueName() + " with type " + javaClass);
			}
			
			return Optional.of(new MapToUDTValueMapConverter(javaClass, userType, repository));
			
		}
		
		else if (abstractDataType instanceof UDTMapDataType) {
			
			UDTMapDataType dt = (UDTMapDataType) abstractDataType;
			
			Class<Object> keyClass = (Class<Object>) dt.getUdtKeyClass();
			Class<Object> valueClass = (Class<Object>) dt.getUdtValueClass();
			
			if (UDTValue.class.isAssignableFrom(keyClass) && UDTValue.class.isAssignableFrom(valueClass)) {
				return Optional.empty();
			}

			UserType keyType = repository.findUserType(dt.getUdtKeyName().getName());
			if (keyType == null) {
				throw new CasserMappingException("UserType not found for " + dt.getUdtKeyName() + " with type " + keyClass);
			}

			UserType valueType = repository.findUserType(dt.getUdtValueName().getName());
			if (valueType == null) {
				throw new CasserMappingException("UserType not found for " + dt.getUdtValueName() + " with type " + valueClass);
			}

			return Optional.of(new MapToUDTMapConverter(keyClass, keyType, valueClass, valueType, repository));
			
		}
		
		return Optional.empty();
	}
	
	
}
