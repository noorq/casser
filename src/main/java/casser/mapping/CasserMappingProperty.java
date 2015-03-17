package casser.mapping;

import java.lang.reflect.Method;

import casser.core.Casser;

public class CasserMappingProperty<E> implements CasserProperty<E> {

	private final CasserMappingEntity<E> entity; 
	
	private final Method getterMethod;
	private Method setterMethod;
	
	private final String propertyName;
	
	private boolean keyInfo = false;
	private boolean isPrimaryKey = false;
	private KeyType keyType = null;
	private int ordinal = 0;
	private Ordering ordering = null;
	
	private String columnName;
	
	public CasserMappingProperty(CasserMappingEntity<E> entity, Method getter) {
		this.entity = entity;
		this.getterMethod = getter;
		this.propertyName = getPropertyName(getter);
	}
	
	@Override
	public CasserMappingEntity<E> getEntity() {
		return entity;
	}
	
	private void ensureKeyInfo() {
		if (!keyInfo) {
			PrimaryKey primaryKey = getterMethod.getDeclaredAnnotation(PrimaryKey.class);
			
			if (primaryKey != null) {
				isPrimaryKey = true;
				keyType = primaryKey.type();
				ordinal = primaryKey.ordinal();
				ordering = primaryKey.ordering();
			}
			
			keyInfo = true;
		}
	}
	
	@Override
	public boolean isPrimaryKey() {
		ensureKeyInfo();
		return isPrimaryKey;
	}

	@Override
	public KeyType getKeyType() {
		ensureKeyInfo();
		return keyType;
	}

	@Override
	public int getOrdinal() {
		ensureKeyInfo();
		return ordinal;
	}

	@Override
	public Ordering getOrdering() {
		ensureKeyInfo();
		return ordering;
	}

	@Override
	public String getColumnName() {
		
		if (columnName == null) {
			
			Column column = getterMethod.getDeclaredAnnotation(Column.class);
			if (column != null) {
				columnName = column.value();
			}
			else {
				PrimaryKey primaryKey = getterMethod.getDeclaredAnnotation(PrimaryKey.class);
				if (primaryKey != null) {
					columnName = primaryKey.value();
				}
			}
			
			if (columnName == null || columnName.isEmpty()) {
				columnName = getDefaultColumnName();
			}
			
		}
		
		return columnName;
	}
	
	private String getDefaultColumnName() {
		return Casser.settings().getPropertyToColumnConverter().apply(propertyName);
	}

	public void setSetterMethod(Method setterMethod) {
		this.setterMethod = setterMethod;
	}

	public String getPropertyName() {
		return propertyName;
	}

	public Method getGetterMethod() {
		return getterMethod;
	}

	public Method getSetterMethod() {
		return setterMethod;
	}
	
	
	public static String getPropertyName(Method m) {
		
		String propertyName = Casser.settings().getMethodNameToPropertyConverter().apply(m.getName());
		
		return propertyName;
	}
}
