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

import java.util.Optional;
import java.util.function.Function;

import casser.core.dsl.Getter;
import casser.core.dsl.Setter;
import casser.support.CasserMappingException;
import casser.support.DslPropertyException;

public final class MappingUtil {

	private MappingUtil() {
	}

	public static Class<?> getMappingInterface(Object entity) {
		
		Class<?> iface = null;
		
		if (entity instanceof Class) {
			iface = (Class<?>) entity;
			
			if (!iface.isInterface()) {
				throw new CasserMappingException("expected interface " + iface);
			}
			
		}
		else {
			Class<?>[] ifaces = entity.getClass().getInterfaces();
			if (ifaces.length != 1) {
				throw new CasserMappingException("supports only single interface, wrong dsl class " + entity.getClass()
						);
			}
			
			iface = ifaces[0];
		}
		
		return iface;
		
	}
	
	public static CasserMappingProperty<?> resolveMappingProperty(Getter<?> getter) {
		
		try {
			getter.get();
			throw new CasserMappingException("getter must reference to a dsl object " + getter);
		}
		catch(DslPropertyException e) {
			return (CasserMappingProperty<?>) e.getProperty();
		}
		
	}

	public static CasserMappingProperty<?> resolveMappingProperty(Setter<?> setter) {
		
		try {
			setter.set(null);
			throw new CasserMappingException("setter must reference to a dsl object " + setter);
		}
		catch(DslPropertyException e) {
			return (CasserMappingProperty<?>) e.getProperty();
		}
		
	}
	public static Object prepareValueForWrite(CasserMappingProperty<?> prop, Object value) {
		
		if (value != null) {
			
			Optional<Function<Object, Object>> converter = prop.getWriteConverter();
			
			if (converter.isPresent()) {
				value = converter.get().apply(value);
			}
			
		}
		
		return value;
	}
	
}
