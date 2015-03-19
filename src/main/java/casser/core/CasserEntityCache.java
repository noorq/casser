package casser.core;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import casser.mapping.CasserMappingEntity;

public class CasserEntityCache {

	private ConcurrentMap<Class<?>, CasserMappingEntity<?>> cache = new ConcurrentHashMap<Class<?>, CasserMappingEntity<?>>();

	public CasserMappingEntity<?> getEntity(Class<?> iface) {
		
		CasserMappingEntity<?> entity = cache.get(iface);
		
		if (entity == null) {
			entity = new CasserMappingEntity(iface);
			
			CasserMappingEntity<?> c = cache.putIfAbsent(iface, entity);
			if (c != null) {
				entity = c;
			}
		}
		
		return entity;
	}
	
}
