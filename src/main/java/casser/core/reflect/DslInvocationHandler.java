package casser.core.reflect;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import casser.config.CasserSettings;
import casser.core.Casser;
import casser.core.ColumnInformation;
import casser.support.DslColumnException;

public class DslInvocationHandler<E> implements InvocationHandler {

	private final Class<E> iface;
	
	public DslInvocationHandler(Class<E> iface) {
		this.iface = iface;
	}
	
	@Override
	public Object invoke(Object proxy, Method method, Object[] args)
			throws Throwable {
		
		if ("toString".equals(method.getName())) {
			return "Casser Dsl for " + iface;
		}
		
		CasserSettings settings = Casser.settings();
		
		String propertyName = settings.getMethodNameToPropertyConverter().apply(method.getName());
		
		String columnName = settings.getPropertyToColumnConverter().apply(propertyName);
		
		ColumnInformation info = new ColumnInformation(columnName);

		throw new DslColumnException(info);
	}

}
