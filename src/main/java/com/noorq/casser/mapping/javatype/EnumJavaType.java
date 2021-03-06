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

import com.datastax.driver.core.DataType;
import com.noorq.casser.core.SessionRepository;
import com.noorq.casser.mapping.ColumnType;
import com.noorq.casser.mapping.convert.EnumToStringConverter;
import com.noorq.casser.mapping.convert.StringToEnumConverter;
import com.noorq.casser.mapping.convert.TypedConverter;
import com.noorq.casser.mapping.type.AbstractDataType;
import com.noorq.casser.mapping.type.DTDataType;

public final class EnumJavaType extends AbstractJavaType {

	@Override
	public Class<?> getJavaClass() {
		return Enum.class;
	}

	@Override
	public AbstractDataType resolveDataType(Method getter, Type genericJavaType, ColumnType columnType) {
		return new DTDataType(columnType, DataType.ascii(), (Class<?>) genericJavaType);
	}

	@Override
	public Optional<Function<Object, Object>> resolveReadConverter(
			AbstractDataType dataType, SessionRepository repository) {

		DTDataType dt = (DTDataType) dataType;
		
		return Optional.of(TypedConverter.create(
				String.class, 
				Enum.class, 
				new StringToEnumConverter(dt.getJavaClass())));
	}

	@Override
	public Optional<Function<Object, Object>> resolveWriteConverter(
			AbstractDataType dataType, SessionRepository repository) {

		return Optional.of(TypedConverter.create(
				Enum.class, 
				String.class, 
				EnumToStringConverter.INSTANCE));
		
	}

}
