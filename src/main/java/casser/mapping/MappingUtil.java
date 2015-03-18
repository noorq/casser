package casser.mapping;

import casser.support.CasserMappingException;

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
	
}
