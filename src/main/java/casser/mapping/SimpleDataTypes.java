/*
 *      Copyright (C) 2015 Noorq Inc.
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

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import casser.support.CasserException;

import com.datastax.driver.core.DataType;

public class SimpleDataTypes {

	private static final Map<Class<?>, Class<?>> wrapperToPrimitiveTypeMap = new HashMap<Class<?>, Class<?>>(8);

	private static final Map<Class<?>, DataType> javaClassToDataTypeMap = new HashMap<Class<?>, DataType>();

	private static final Map<DataType.Name, DataType> nameToDataTypeMap = new HashMap<DataType.Name, DataType>();

	static {

		wrapperToPrimitiveTypeMap.put(Boolean.class, boolean.class);
		wrapperToPrimitiveTypeMap.put(Byte.class, byte.class);
		wrapperToPrimitiveTypeMap.put(Character.class, char.class);
		wrapperToPrimitiveTypeMap.put(Double.class, double.class);
		wrapperToPrimitiveTypeMap.put(Float.class, float.class);
		wrapperToPrimitiveTypeMap.put(Integer.class, int.class);
		wrapperToPrimitiveTypeMap.put(Long.class, long.class);
		wrapperToPrimitiveTypeMap.put(Short.class, short.class);

		Set<Class<?>> simpleTypes = new HashSet<Class<?>>();

		for (DataType dataType : DataType.allPrimitiveTypes()) {

			if (dataType.equals(DataType.counter()) || dataType.equals(DataType.ascii()) || dataType.equals(DataType.uuid())) {
				continue;
			}
			
			Class<?> javaClass = dataType.asJavaClass();
			simpleTypes.add(javaClass);
			
			DataType dt = javaClassToDataTypeMap.putIfAbsent(javaClass, dataType);
			if (dt != null) {
				throw new IllegalStateException("java type " + javaClass + " is has two datatypes " + dt + " and " + dataType);
			}

			Class<?> primitiveJavaClass = wrapperToPrimitiveTypeMap.get(javaClass);
			if (primitiveJavaClass != null) {
				javaClassToDataTypeMap.put(primitiveJavaClass, dataType);
				simpleTypes.add(primitiveJavaClass);
			}

			nameToDataTypeMap.put(dataType.getName(), dataType);
		}

		javaClassToDataTypeMap.put(String.class, DataType.text());
		javaClassToDataTypeMap.put(Enum.class, DataType.ascii());

	}

	public static DataType getDataTypeByName(DataType.Name name) {
		return nameToDataTypeMap.get(name);
	}

	public static DataType getDataTypeByJavaClass(Type type) {
		
		Class<?> javaClass = (Class<?>) type;
		
		return javaClassToDataTypeMap.get(javaClass);
	}

	public static DataType.Name[] getDataTypeNamesForArguments(Type[] arguments) {

		DataType.Name[] result = new DataType.Name[arguments.length];

		for (int i = 0; i != result.length; ++i) {

			Type type = arguments[i];

			Class<?> javaClass = (Class<?>) type;

			DataType dataType = getDataTypeByJavaClass(javaClass);

			if (dataType == null) {
				throw new CasserException("not found appropriate DataType for javaClass=" + javaClass);
			}

			result[i] = dataType.getName();
		}

		return result;
	}

}
