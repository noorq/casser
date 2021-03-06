/*
 *      Copyright (C) 2015 The Casser Authors
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
package com.noorq.casser.mapping.convert.udt;

import java.util.Map;
import java.util.function.Function;

import com.datastax.driver.core.UDTValue;
import com.noorq.casser.core.SessionRepository;
import com.noorq.casser.mapping.convert.ProxyValueReader;
import com.noorq.casser.mapping.value.UDTColumnValueProvider;
import com.noorq.casser.support.Transformers;

public final class UDTKeyMapToMapConverter implements Function<Object, Object> {

	final ProxyValueReader<UDTValue> reader;
	
	public UDTKeyMapToMapConverter(Class<?> iface, SessionRepository repository) {
		this.reader = new ProxyValueReader<UDTValue>(iface, new UDTColumnValueProvider(repository));
	}

	@Override
	public Object apply(Object t) {
		return Transformers.transformMapKey((Map<UDTValue, Object>) t, reader);
	}

}
