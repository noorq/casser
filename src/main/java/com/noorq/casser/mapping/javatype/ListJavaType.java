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
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.noorq.casser.core.SessionRepository;
import com.noorq.casser.mapping.ColumnType;
import com.noorq.casser.mapping.IdentityName;
import com.noorq.casser.mapping.annotation.Types;
import com.noorq.casser.mapping.convert.ListToTupleListConverter;
import com.noorq.casser.mapping.convert.ListToUDTListConverter;
import com.noorq.casser.mapping.convert.TupleListToListConverter;
import com.noorq.casser.mapping.convert.UDTListToListConverter;
import com.noorq.casser.mapping.type.AbstractDataType;
import com.noorq.casser.mapping.type.DTDataType;
import com.noorq.casser.mapping.type.UDTListDataType;
import com.noorq.casser.support.CasserMappingException;
import com.noorq.casser.support.Either;

public final class ListJavaType extends AbstractJavaType {

	@Override
	public Class<?> getJavaClass() {
		return List.class;
	}

	@Override
	public AbstractDataType resolveDataType(Method getter, Type genericJavaType, ColumnType columnType) {

		Types.List clist = getter.getDeclaredAnnotation(Types.List.class);
		if (clist != null) {
			return new DTDataType(columnType, 
					DataType.list(resolveSimpleType(getter, clist.value())));
		}

		Types.UDTList udtList = getter.getDeclaredAnnotation(Types.UDTList.class);
		if (udtList != null) {
			return new UDTListDataType(columnType, 
					resolveUDT(udtList.value()),
					UDTValue.class);
		}

		Type[] args = getTypeParameters(genericJavaType);
		ensureTypeArguments(getter, args.length, 1);
		
		Either<DataType, IdentityName> parameterType = autodetectParameterType(getter, args[0]);

		if (parameterType.isLeft()) {
			return DTDataType.list(columnType, parameterType.getLeft(), args[0]);
		}
		else {
			return new UDTListDataType(columnType, 
					parameterType.getRight(),
					(Class<?>) args[0]);
		}
		
	}
	
	@Override
	public Optional<Function<Object, Object>> resolveReadConverter(
			AbstractDataType abstractDataType, SessionRepository repository) {
		
		if (abstractDataType instanceof DTDataType) {
			
			DTDataType dt = (DTDataType) abstractDataType;
			DataType elementType = dt.getDataType().getTypeArguments().get(0);
			if (elementType instanceof TupleType) {
			
				Class<?> tupleClass = dt.getTypeArguments()[0];
				
				if (TupleValue.class.isAssignableFrom(tupleClass)) {
					return Optional.empty();
				}
				
				return Optional.of(new TupleListToListConverter(tupleClass, repository));
			}
		}
		
		else if (abstractDataType instanceof UDTListDataType) {
			
			UDTListDataType dt = (UDTListDataType) abstractDataType;
			
			Class<Object> javaClass = (Class<Object>) dt.getTypeArguments()[0];
			
			if (UDTValue.class.isAssignableFrom(javaClass)) {
				return Optional.empty();
			}
			
			return Optional.of(new UDTListToListConverter(javaClass, repository));
			
		}
		
		return Optional.empty();
	}

	@Override
	public Optional<Function<Object, Object>> resolveWriteConverter(
			AbstractDataType abstractDataType, SessionRepository repository) {
		
		if (abstractDataType instanceof DTDataType) {
			
			DTDataType dt = (DTDataType) abstractDataType;
			DataType elementType = dt.getDataType().getTypeArguments().get(0);
			
			if (elementType instanceof TupleType) {
				
				Class<?> tupleClass = dt.getTypeArguments()[0];
				
				if (TupleValue.class.isAssignableFrom(tupleClass)) {
					return Optional.empty();
				}
				
				return Optional.of(new ListToTupleListConverter(tupleClass, (TupleType) elementType, repository));
			}
			
		}
		
		else if (abstractDataType instanceof UDTListDataType) {
			
			UDTListDataType dt = (UDTListDataType) abstractDataType;
			
			Class<Object> javaClass = (Class<Object>) dt.getTypeArguments()[0];
			
			if (UDTValue.class.isAssignableFrom(javaClass)) {
				return Optional.empty();
			}

			UserType userType = repository.findUserType(dt.getUdtName().getName());
			if (userType == null) {
				throw new CasserMappingException("UserType not found for " + dt.getUdtName() + " with type " + javaClass);
			}
			
			return Optional.of(new ListToUDTListConverter(javaClass, userType, repository));
			
		}
		
		return Optional.empty();
	}
	
}
