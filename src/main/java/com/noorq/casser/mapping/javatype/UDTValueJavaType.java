/*
 *      Copyright (C) 2015 The Casser Authors
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
import java.util.Optional;
import java.util.function.Function;

import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.noorq.casser.core.SessionRepository;
import com.noorq.casser.mapping.ColumnType;
import com.noorq.casser.mapping.IdentityName;
import com.noorq.casser.mapping.MappingUtil;
import com.noorq.casser.mapping.annotation.Types;
import com.noorq.casser.mapping.convert.TypedConverter;
import com.noorq.casser.mapping.convert.udt.EntityToUDTValueConverter;
import com.noorq.casser.mapping.convert.udt.UDTValueToEntityConverter;
import com.noorq.casser.mapping.type.AbstractDataType;
import com.noorq.casser.mapping.type.UDTDataType;
import com.noorq.casser.support.CasserMappingException;

public final class UDTValueJavaType extends AbstractJavaType {

	@Override
	public Class<?> getJavaClass() {
		return UDTValue.class;
	}

	@Override
	public boolean isApplicable(Class<?> javaClass) {
		return MappingUtil.isUDT(javaClass);
	}
	
	@Override
	public AbstractDataType resolveDataType(Method getter, Type genericJavaType, ColumnType columnType) {
	
		Class<?> javaType = (Class<?>) genericJavaType;
		
		IdentityName udtName = null;
		
		if (UDTValue.class.isAssignableFrom(javaType)) {
			
			Types.UDT userTypeName = getter.getDeclaredAnnotation(Types.UDT.class);
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

	@Override
	public Optional<Function<Object, Object>> resolveReadConverter(
			AbstractDataType dataType, SessionRepository repository) {

		UDTDataType dt = (UDTDataType) dataType;
		
		Class<Object> javaClass = (Class<Object>) dt.getTypeArguments()[0];
		
		if (UDTValue.class.isAssignableFrom(javaClass)) {
			return Optional.empty();
		}
		
		return Optional.of(TypedConverter.create(
				UDTValue.class,
				javaClass,
				new UDTValueToEntityConverter(javaClass, repository)));
	}

	@Override
	public Optional<Function<Object, Object>> resolveWriteConverter(
			AbstractDataType dataType, SessionRepository repository) {

		UDTDataType dt = (UDTDataType) dataType;
		
		Class<Object> javaClass = (Class<Object>) dt.getTypeArguments()[0];
		
		if (UDTValue.class.isAssignableFrom(javaClass)) {
			return Optional.empty();
		}

		UserType userType = repository.findUserType(dt.getUdtName().getName());
		if (userType == null) {
			throw new CasserMappingException("UserType not found for " + dt.getUdtName() + " with type " + javaClass);
		}
		
		return Optional.of(TypedConverter.create(
				javaClass, 
				UDTValue.class, 
				new EntityToUDTValueConverter(javaClass, userType, repository)));
		
	}

}
