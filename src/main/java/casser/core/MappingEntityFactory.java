package casser.core;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import casser.mapping.CasserMappingEntity;

public class MappingEntityFactory {

	private ConcurrentMap<Class<?>, CasserMappingEntity<?>> knownEntities = new ConcurrentHashMap<Class<?>, CasserMappingEntity<?>>();

	public CasserMappingEntity<?> getEntity(Class<?> iface) {
		
		CasserMappingEntity<?> entity = knownEntities.get(iface);
		
		if (entity == null) {
			entity = new CasserMappingEntity(iface);
			
			CasserMappingEntity<?> c = knownEntities.putIfAbsent(iface, entity);
			if (c != null) {
				entity = c;
			}
		}
		
		return entity;
	}
	
}
