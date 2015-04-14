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
package com.noorq.casser.mapping.value;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.Function;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.noorq.casser.core.SessionRepository;
import com.noorq.casser.mapping.CasserProperty;

public final class TupleColumnValueProvider implements ColumnValueProvider {

	private final SessionRepository repository;
	
	public TupleColumnValueProvider(SessionRepository repository) {
		this.repository = repository;
	}
	
	@Override
	public <V> V getColumnValue(Object sourceObj, int columnIndexUnused, CasserProperty property) {

		int columnIndex = property.getOrdinal();
		
		TupleValue source = (TupleValue) sourceObj;
		TupleType tupleType = source.getType();

		ByteBuffer bytes = source.getBytesUnsafe(columnIndex);
		DataType fieldType = tupleType.getComponentTypes().get(columnIndex);

		Object value = fieldType.deserialize(bytes, ProtocolVersion.NEWEST_SUPPORTED);
		
		if (value != null) {

			Optional<Function<Object, Object>> converter = property.getReadConverter(repository);
			
			if (converter.isPresent()) {
				value = converter.get().apply(value);
			}
			
		}
		
		return (V) value;
		
	}
}
