/*
 *      Copyright (C) 2015 Noorq, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
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

	public Class<E> getMappingInterface() {
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
				if (table.forceQuote()) {
					tableName = CqlUtil.forceQuote(tableName);
				}
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
