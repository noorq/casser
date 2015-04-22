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
import java.util.function.Function;

import com.datastax.driver.core.UDTValue;
import com.google.common.collect.ImmutableMap;
import com.noorq.casser.core.Casser;
import com.noorq.casser.core.SessionRepository;
import com.noorq.casser.mapping.CasserEntity;
import com.noorq.casser.mapping.value.UDTColumnValueProvider;
import com.noorq.casser.mapping.value.ValueProviderMap;

public final class UDTMapToMapConverter implements Function<Object, Object> {

	private final Class<?> keyClass;
	private final CasserEntity keyEntity;
	private final Class<?> valueClass;
	private final CasserEntity valueEntity;
	private final UDTColumnValueProvider valueProvider;
	
	public UDTMapToMapConverter(Class<?> keyClass, Class<?> valueClass, SessionRepository repository) {
		this.keyClass = keyClass;
		this.keyEntity = Casser.entity(keyClass);
		this.valueClass = valueClass;
		this.valueEntity = Casser.entity(valueClass);
		this.valueProvider = new UDTColumnValueProvider(repository);
	}

	@Override
	public Object apply(Object t) {
		
		Map<UDTValue, UDTValue> sourceMap = (Map<UDTValue, UDTValue>) t;
		
		ImmutableMap.Builder<Object, Object> builder = ImmutableMap.builder();
		
		for (Map.Entry<UDTValue, UDTValue> source : sourceMap.entrySet()) {
		
			Object keyObj = null;
			Object valueObj = null;
			
			if (source.getKey() != null) {
			
				Map<String, Object> map = new ValueProviderMap(source.getKey(), 
						valueProvider,
						keyEntity);
				
				keyObj = Casser.map(keyClass, map);
				
			}

			if (source.getValue() != null) {
				
				Map<String, Object> map = new ValueProviderMap(source.getValue(), 
						valueProvider,
						valueEntity);
				
				valueObj = Casser.map(valueClass, map);
				
			}

			builder.put(keyObj, valueObj);
		
		}

		return builder.build();
		
	}

}
