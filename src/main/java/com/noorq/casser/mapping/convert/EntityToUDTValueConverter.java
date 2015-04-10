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
package com.noorq.casser.mapping.convert;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Function;

import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.noorq.casser.core.Casser;
import com.noorq.casser.core.SessionRepository;
import com.noorq.casser.core.reflect.MapExportable;
import com.noorq.casser.mapping.CasserMappingEntity;
import com.noorq.casser.mapping.CasserMappingProperty;
import com.noorq.casser.mapping.value.BeanColumnValueProvider;
import com.noorq.casser.mapping.value.UDTColumnValuePreparer;
import com.noorq.casser.support.CasserMappingException;

public final class EntityToUDTValueConverter implements Function<Object, UDTValue> {

	private final SessionRepository repository;
	private final UserType userType;
	private final CasserMappingEntity entity;
	private final UDTColumnValuePreparer valuePreparer;
	
	public EntityToUDTValueConverter(Class<?> iface, UserType userType, SessionRepository repository) {
		
		this.repository = repository;
		this.userType = userType;
		this.entity = Casser.entity(iface);
		
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
				
				CasserMappingProperty prop = entity.getMappingProperty(entry.getKey());
				
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
		
			udtValue.setBytesUnsafe(prop.getColumnName().getName(), bytes);
			
		}
	}

}
