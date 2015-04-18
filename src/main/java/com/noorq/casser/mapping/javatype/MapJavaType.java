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

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.UDTValue;
import com.noorq.casser.mapping.ColumnType;
import com.noorq.casser.mapping.IdentityName;
import com.noorq.casser.mapping.annotation.Types;
import com.noorq.casser.mapping.type.AbstractDataType;
import com.noorq.casser.mapping.type.DTDataType;
import com.noorq.casser.mapping.type.UDTValueMapDataType;
import com.noorq.casser.mapping.type.UDTKeyMapDataType;
import com.noorq.casser.mapping.type.UDTMapDataType;
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
	
}
