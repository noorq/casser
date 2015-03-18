package casser.mapping;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import casser.config.CasserSettings;
import casser.core.Casser;

public class CasserMappingEntity<E> implements CasserEntity<E> {

	private final Class<E> iface;
	private String tableName;
	private final Map<String, CasserMappingProperty<E>> props = new HashMap<String, CasserMappingProperty<E>>();
	
	public CasserMappingEntity(Class<E> iface) {
		
		if (iface == null || !iface.isInterface()) {
			throw new IllegalArgumentException("invalid parameter " + iface);
		}
		
		this.iface = iface;
		
		CasserSettings settings = Casser.settings();
		
		Method[] all = iface.getDeclaredMethods();
		
		for (Method m : all) {
			
			if (settings.getGetterMethodDetector().apply(m)) {
				
				CasserMappingProperty<E> prop = new CasserMappingProperty<E>(this, m);
				
				props.put(prop.getPropertyName(), prop);
				
			}
			
		}

		for (Method m : all) {
			
			if (settings.getSetterMethodDetector().apply(m)) {
				
				String propertyName = CasserMappingProperty.getPropertyName(m);

				CasserMappingProperty<E> prop = props.get(propertyName);
				
				if (prop != null) {
					prop.setSetterMethod(m);
				}
				
			}
			
		}

	}

	public Class<E> getEntityInterface() {
		return iface;
	}	
	
	@Override
	public String getName() {
		return iface.toString();
	}

	@Override
	public Collection<CasserProperty<E>> getProperties() {
		return Collections.unmodifiableCollection(props.values());
	}

	public Collection<CasserMappingProperty<E>> getMappingProperties() {
		return Collections.unmodifiableCollection(props.values());
	}

	@Override
	public String getTableName() {
		
		if (tableName == null) {
			
			Table table = iface.getDeclaredAnnotation(Table.class);
			
			if (table != null) {
				tableName = table.value();
			}
			
			if (tableName == null || tableName.isEmpty()) {
				
				tableName = getDefaultTableName();
				
			}
		}
		
		return tableName;
	}
	
	private String getDefaultTableName() {
		return Casser.settings().getPropertyToColumnConverter().apply(iface.getSimpleName());
	}
	
}
