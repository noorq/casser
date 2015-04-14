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

import java.util.Map;

import com.noorq.casser.core.Casser;
import com.noorq.casser.core.reflect.MapExportable;
import com.noorq.casser.mapping.CasserEntity;
import com.noorq.casser.mapping.CasserProperty;
import com.noorq.casser.mapping.value.BeanColumnValueProvider;

public abstract class AbstractEntityValueWriter<V> {

	abstract void writeColumn(V outValue, Object value,
			CasserProperty prop);

	final CasserEntity entity;
	
	public AbstractEntityValueWriter(Class<?> iface) {
		this.entity = Casser.entity(iface);
	}
	
	public void write(V outValue, Object source) {
		
		if (source instanceof MapExportable) {
			
			MapExportable exportable = (MapExportable) source;
			
			Map<String, Object> propertyToValueMap = exportable.toMap();
			
			for (Map.Entry<String, Object> entry : propertyToValueMap.entrySet()) {
				
				Object value = entry.getValue();
				
				if (value == null) {
					continue;
				}
				
				CasserProperty prop = entity.getProperty(entry.getKey());
				
				if (prop != null) {
					
					writeColumn(outValue, value, prop);
					
				}
				
			}
			
		}
		else {

			for (CasserProperty prop : entity.getOrderedProperties()) {
				
				Object value = BeanColumnValueProvider.INSTANCE.getColumnValue(source, -1, prop);
				
				if (value != null) {
					writeColumn(outValue, value, prop);
				}
				
			}
			
		}

	}

	
}
