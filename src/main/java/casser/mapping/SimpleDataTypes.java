package casser.mapping;

import java.lang.reflect.Type;
import java.util.Collections;
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

	private static final Set<Class<?>> CASSANDRA_SIMPLE_TYPES;

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

		CASSANDRA_SIMPLE_TYPES = Collections.unmodifiableSet(simpleTypes);

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
