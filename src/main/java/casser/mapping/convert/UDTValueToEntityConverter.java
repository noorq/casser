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

import java.util.Map;
import java.util.function.Function;

import casser.core.Casser;
import casser.mapping.CasserMappingEntity;
import casser.mapping.CasserMappingRepository;
import casser.mapping.UDTValueMap;
import casser.mapping.value.UDTColumnValueProvider;
import casser.support.CasserMappingException;

import com.datastax.driver.core.UDTValue;

public final class UDTValueToEntityConverter implements Function<UDTValue, Object> {

	private final Class<?> iface;
	private final CasserMappingRepository repository;
	private final CasserMappingEntity entity;
	
	public UDTValueToEntityConverter(Class<?> iface, CasserMappingRepository repository) {
		this.iface = iface;
		this.repository = repository;
		this.entity = repository.getEntity(iface);
		
		if (this.entity == null) {
			throw new CasserMappingException("entity not found for " + iface);
		}
	}

	@Override
	public Object apply(UDTValue source) {
		
		Map<String, Object> map = new UDTValueMap(source, 
				new UDTColumnValueProvider(repository),
				entity);
		
		return Casser.wrap(map, iface);
		
	}

}
