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
package casser.mapping.convert;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Function;

import casser.mapping.CasserMappingEntity;
import casser.mapping.CasserMappingProperty;
import casser.mapping.CasserMappingRepository;
import casser.mapping.MapExportable;
import casser.mapping.value.BeanColumnValueProvider;
import casser.mapping.value.UDTColumnValuePreparer;
import casser.support.CasserMappingException;

import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;

public final class EntityToUDTValueConverter implements Function<Object, UDTValue> {

	private final CasserMappingRepository repository;
	private final UserType userType;
	private final CasserMappingEntity entity;
	private final UDTColumnValuePreparer valuePreparer;
	
	public EntityToUDTValueConverter(Class<?> iface, String udtName, CasserMappingRepository repository) {
		
		this.repository = repository;
		
		this.userType = repository.findUserType(udtName);
		if (this.userType == null) {
			throw new CasserMappingException("UserType not found for " + udtName + " with type " + iface);
		}
		
		this.entity = repository.getEntity(iface);
		
		if (this.entity == null) {
			throw new CasserMappingException("entity not found for " + iface);
		}
		
		this.valuePreparer = new UDTColumnValuePreparer(this.userType, this.repository);
	}
	
	@Override
	public UDTValue apply(Object source) {
		
		UDTValue udtValue = userType.newValue();
		
		if (source instanceof MapExportable) {
			
			MapExportable exportable = (MapExportable) source;
			
			Map<String, Object> propertyToValueMap = exportable.toMap();
			
			for (Map.Entry<String, Object> entry : propertyToValueMap.entrySet()) {
				
				Object value = entry.getValue();
				
				if (value == null) {
					continue;
				}
				
				CasserMappingProperty prop = entity.getMappingProperty(entity.getName());
				
				if (prop != null) {
					
					write(udtValue, value, prop);
					
				}
				
			}
			
		}
		else {

			for (CasserMappingProperty prop : entity.getMappingProperties()) {
				
				Object value = BeanColumnValueProvider.INSTANCE.getColumnValue(source, -1, prop);
				
				if (value != null) {
					write(udtValue, value, prop);
				}
				
			}
			
		}

		return udtValue;
	}

	private void write(UDTValue udtValue, Object value,
			CasserMappingProperty prop) {
		
		ByteBuffer bytes = (ByteBuffer) valuePreparer.prepareColumnValue(value, prop);
		
		if (bytes != null) {
		
			udtValue.setBytesUnsafe(prop.getColumnName(), bytes);
			
		}
	}

}
