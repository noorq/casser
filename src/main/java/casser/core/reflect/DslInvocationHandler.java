package casser.core.reflect;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import casser.config.CasserSettings;
import casser.core.Casser;
import casser.core.ColumnInformation;
import casser.core.PrimaryKeyInformation;
import casser.mapping.Column;
import casser.mapping.PrimaryKey;
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
		
		String columnName = null;
		
		PrimaryKey primaryKey = method.getDeclaredAnnotation(PrimaryKey.class);
		if (primaryKey != null) {
			columnName = primaryKey.value();
			
			if (columnName == null || columnName.isEmpty()) {
				columnName = getDefaultColumnName(method);
			}
			
			PrimaryKeyInformation info = new PrimaryKeyInformation(columnName);
			throw new DslColumnException(info);
		}
		
		Column column = method.getDeclaredAnnotation(Column.class);
		if (column != null) {
			columnName = column.value();
		}
		
		if (columnName == null || columnName.isEmpty()) {
			columnName = getDefaultColumnName(method);
		}
		
		ColumnInformation info = new ColumnInformation(columnName);

		throw new DslColumnException(info);
	}
	
	private String getDefaultColumnName(Method method) {
		
		CasserSettings settings = Casser.settings();
		
		String propertyName = settings.getMethodNameToPropertyConverter().apply(method.getName());
		
		return settings.getPropertyToColumnConverter().apply(propertyName);
	}

}
