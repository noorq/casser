package casser.core.reflect;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import casser.mapping.CasserMappingEntity;
import casser.mapping.CasserMappingProperty;
import casser.support.CasserException;
import casser.support.DslPropertyException;

public class DslInvocationHandler<E> implements InvocationHandler {

	private final CasserMappingEntity<E> entity;
	
	private final Map<Method, CasserMappingProperty<E>> map = new HashMap<Method, CasserMappingProperty<E>>();
	
	public DslInvocationHandler(Class<E> iface) {
		
		this.entity = new CasserMappingEntity<E>(iface);
		
		for (CasserMappingProperty<E> prop : entity.getMappingProperties()) {
			
			map.put(prop.getGetterMethod(), prop);
			
			Method setter = prop.getSetterMethod();
			
			if (setter != null) {
				map.put(setter, prop);
			}
			
		}
	}
	
	@Override
	public Object invoke(Object proxy, Method method, Object[] args)
			throws Throwable {
		
		if ("toString".equals(method.getName())) {
			return "Casser Dsl for " + entity.getMappingInterface();
		}
		
		CasserMappingProperty<E> prop = map.get(method);
		
		if (prop != null) {
			throw new DslPropertyException(prop);	
		}
		
		throw new CasserException("invalid method call " + method);
	}

}
