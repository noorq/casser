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

import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.DataType;

public final class SimpleJavaTypes {

	private static final Map<Class<?>, DataType> javaClassToDataTypeMap = new HashMap<Class<?>, DataType>();

	private static final Map<DataType.Name, DataType> nameToDataTypeMap = new HashMap<DataType.Name, DataType>();

	static {

		for (DataType dataType : DataType.allPrimitiveTypes()) {

			nameToDataTypeMap.put(dataType.getName(), dataType);
			
			if (dataType.equals(DataType.counter()) || 
				dataType.equals(DataType.ascii()) || 
				dataType.equals(DataType.timeuuid()
					)) {
				continue;
			}
			
			Class<?> javaClass = dataType.asJavaClass();
			
			DataType dt = javaClassToDataTypeMap.putIfAbsent(javaClass, dataType);
			if (dt != null) {
				throw new IllegalStateException("java type " + javaClass + " is has two datatypes " + dt + " and " + dataType);
			}

		}

		javaClassToDataTypeMap.put(String.class, DataType.text());

	}

	private SimpleJavaTypes() {
	}
	
	public static DataType getDataTypeByName(DataType.Name name) {
		return nameToDataTypeMap.get(name);
	}

	public static DataType getDataTypeByJavaClass(Class<?> javaType) {
		return javaClassToDataTypeMap.get(javaType);
	}


}
