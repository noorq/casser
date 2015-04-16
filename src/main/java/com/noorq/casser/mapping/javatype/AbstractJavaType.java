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
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Optional;
import java.util.function.Function;

import com.datastax.driver.core.DataType;
import com.noorq.casser.core.SessionRepository;
import com.noorq.casser.mapping.ColumnType;
import com.noorq.casser.mapping.IdentityName;
import com.noorq.casser.mapping.MappingUtil;
import com.noorq.casser.mapping.annotation.T;
import com.noorq.casser.mapping.type.AbstractDataType;
import com.noorq.casser.support.CasserMappingException;
import com.noorq.casser.support.Either;

public abstract class AbstractJavaType {

	public abstract Class<?> getJavaClass();
	
	public boolean isApplicable(Class<?> javaClass) {
		return false;
	}
	
	public abstract AbstractDataType resolveDataType(Method getter, Type genericJavaType, ColumnType columnType);

	public Optional<Class<?>> getPrimitiveJavaClass() {
		return Optional.empty();
	}

	public Optional<Function<Object, Object>> resolveReadConverter(AbstractDataType dataType,
			SessionRepository repository) {
		return Optional.empty();
	}

	public Optional<Function<Object, Object>> resolveWriteConverter(AbstractDataType dataType,
			SessionRepository repository) {
		return Optional.empty();
	}
	
	static IdentityName resolveUDT(T.UDT annotation) {
		return IdentityName.of(annotation.value(), annotation.forceQuote());
	}
	
	static DataType resolveSimpleType(Method getter, DataType.Name typeName) {
		DataType dataType = SimpleJavaTypes.getDataTypeByName(typeName);
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
	
	static Either<DataType, IdentityName> autodetectParameterType(Method getter, Type type) {
		
		DataType dataType = null;
		
		if (type instanceof Class<?>) {
			
			Class<?> javaType = (Class<?>) type;
			dataType = SimpleJavaTypes.getDataTypeByJavaClass(javaType);

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
	
	static Type[] getTypeParameters(Type genericJavaType) {
		
		if (genericJavaType instanceof ParameterizedType) {
		
			ParameterizedType type = (ParameterizedType) genericJavaType;
		
			return type.getActualTypeArguments();
		}
		
		return new Type[] {};
	}
	
}
