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

import com.datastax.driver.core.TupleValue;
import com.noorq.casser.core.Casser;
import com.noorq.casser.core.SessionRepository;
import com.noorq.casser.mapping.CasserEntity;
import com.noorq.casser.mapping.value.TupleColumnValueProvider;
import com.noorq.casser.mapping.value.ValueProviderMap;

public final class TupleValueToEntityConverter implements Function<TupleValue, Object> {

	private final Class<?> iface;
	private final SessionRepository repository;
	private final CasserEntity entity;
	
	public TupleValueToEntityConverter(Class<?> iface, SessionRepository repository) {
		this.iface = iface;
		this.repository = repository;
		this.entity = Casser.entity(iface);
	}

	@Override
	public Object apply(TupleValue source) {
		
		Map<String, Object> map = new ValueProviderMap(source, 
				new TupleColumnValueProvider(repository),
				entity);
		
		return Casser.map(iface, map);
		
	}
}
