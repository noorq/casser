package casser.mapping;

import java.util.Optional;
import java.util.function.Function;

import casser.core.dsl.Getter;
import casser.support.CasserMappingException;
import casser.support.DslPropertyException;

public final class MappingUtil {

	private MappingUtil() {
	}

	public static Class<?> getMappingInterface(Object entity) {
		
		Class<?> iface = null;
		
		if (entity instanceof Class) {
			iface = (Class<?>) entity;
			
			if (!iface.isInterface()) {
				throw new CasserMappingException("expected interface " + iface);
			}
			
		}
		else {
			Class<?>[] ifaces = entity.getClass().getInterfaces();
			if (ifaces.length != 1) {
				throw new CasserMappingException("supports only single interface, wrong dsl class " + entity.getClass()
						);
			}
			
			iface = ifaces[0];
		}
		
		return iface;
		
	}
	
	public static CasserMappingProperty<?> resolveMappingProperty(Getter<?> getter) {
		
		try {
			getter.get();
			throw new CasserMappingException("getter must reference to dsl object " + getter);
		}
		catch(DslPropertyException e) {
			return (CasserMappingProperty<?>) e.getProperty();
		}
		
	}
	
	public static Object prepareValueForWrite(CasserMappingProperty<?> prop, Object value) {
		
		if (value != null) {
			
			Optional<Function<Object, Object>> converter = prop.getWriteConverter();
			
			if (converter.isPresent()) {
				value = converter.get().apply(value);
			}
			
		}
		
		return value;
	}
	
}
