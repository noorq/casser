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
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.noorq.casser.mapping.CasserMappingProperty;
import com.noorq.casser.mapping.CasserMappingRepository;

public final class RowColumnValueProvider implements ColumnValueProvider {

	private final CasserMappingRepository repository;
	
	public RowColumnValueProvider(CasserMappingRepository repository) {
		this.repository = repository;
	}
	
	@Override
	public <V> V getColumnValue(Object sourceObj, int columnIndex, CasserMappingProperty property) {

		Row source = (Row) sourceObj;
		
		if (source.isNull(columnIndex)) {
			return null;
		}
		
		ColumnDefinitions columnDefinitions = source.getColumnDefinitions();
		
		DataType columnType = columnDefinitions.getType(columnIndex);

		if (columnType.isCollection()) {

			List<DataType> typeArguments = columnType.getTypeArguments();

			switch (columnType.getName()) {
			case SET:
				return (V) source.getSet(columnIndex, typeArguments.get(0).asJavaClass());
			case MAP:
				return (V) source.getMap(columnIndex, typeArguments.get(0).asJavaClass(), typeArguments.get(1).asJavaClass());
			case LIST:
				return (V) source.getList(columnIndex, typeArguments.get(0).asJavaClass());
			}

		}

		ByteBuffer bytes = source.getBytesUnsafe(columnIndex);
		Object value = columnType.deserialize(bytes, ProtocolVersion.NEWEST_SUPPORTED);

		if (value != null) {

			Optional<Function<Object, Object>> converter = property.getReadConverter(repository);
			
			if (converter.isPresent()) {
				value = converter.get().apply(value);
			}
			
		}

		return (V) value;
	}

}
