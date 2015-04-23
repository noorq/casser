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

import com.noorq.casser.core.Casser;
import com.noorq.casser.mapping.CasserEntity;
import com.noorq.casser.mapping.value.ColumnValueProvider;
import com.noorq.casser.mapping.value.ValueProviderMap;

public class ProxyValueReader<T> implements Function<T, Object> {

	private final Class<?> iface;
	private final CasserEntity entity;
	private final ColumnValueProvider valueProvider;
	
	public ProxyValueReader(Class<?> iface, ColumnValueProvider valueProvider) {
		this.iface = iface;
		this.entity = Casser.entity(iface);
		this.valueProvider = valueProvider;
	}
	
	@Override
	public Object apply(T source) {
		if (source != null) {
			Map<String, Object> map = new ValueProviderMap(source, 
					valueProvider,
					entity);
			
			return Casser.map(iface, map);
		}
		return null;
	}

}
