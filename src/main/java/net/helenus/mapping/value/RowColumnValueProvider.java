/*
 *      Copyright (C) 2015 The Helenus Authors
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
package net.helenus.mapping.value;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import com.datastax.driver.core.*;

import net.helenus.core.SessionRepository;
import net.helenus.mapping.HelenusProperty;

public final class RowColumnValueProvider implements ColumnValueProvider {

	private final SessionRepository repository;

	public RowColumnValueProvider(SessionRepository repository) {
		this.repository = repository;
	}

	@Override
	public <V> V getColumnValue(Object sourceObj, int columnIndex, HelenusProperty property) {

		Row source = (Row) sourceObj;

		Object value = null;
		if (columnIndex != -1) {
			value = readValueByIndex(source, columnIndex);
		} else {
			value = readValueByName(source, property.getColumnName().getName());
		}

		if (value != null) {

			Optional<Function<Object, Object>> converter = property.getReadConverter(repository);

			if (converter.isPresent()) {
				value = converter.get().apply(value);
			}

		}

		return (V) value;
	}

	private Object readValueByIndex(Row source, int columnIndex) {

		if (source.isNull(columnIndex)) {
			return null;
		}

		ColumnDefinitions columnDefinitions = source.getColumnDefinitions();

		DataType columnType = columnDefinitions.getType(columnIndex);

		if (columnType.isCollection()) {

			List<DataType> typeArguments = columnType.getTypeArguments();

			switch (columnType.getName()) {
				case SET :
					return source.getSet(columnIndex, codecFor(typeArguments.get(0)).getJavaType());
				case MAP :
					return source.getMap(columnIndex, codecFor(typeArguments.get(0)).getJavaType(),
                            codecFor(typeArguments.get(1)).getJavaType());
				case LIST :
					return source.getList(columnIndex, codecFor(typeArguments.get(0)).getJavaType());
			}

		}

		ByteBuffer bytes = source.getBytesUnsafe(columnIndex);
		Object value = codecFor(columnType).deserialize(bytes, ProtocolVersion.NEWEST_SUPPORTED);

		return value;
	}

	private Object readValueByName(Row source, String columnName) {

		if (source.isNull(columnName)) {
			return null;
		}

		ColumnDefinitions columnDefinitions = source.getColumnDefinitions();

		DataType columnType = columnDefinitions.getType(columnName);

		if (columnType.isCollection()) {

			List<DataType> typeArguments = columnType.getTypeArguments();

			switch (columnType.getName()) {
				case SET :
					return source.getSet(columnName, codecFor(typeArguments.get(0)).getJavaType());
				case MAP :
					return source.getMap(columnName, codecFor(typeArguments.get(0)).getJavaType(),
                            codecFor(typeArguments.get(1)).getJavaType());
				case LIST :
					return source.getList(columnName, codecFor(typeArguments.get(0)).getJavaType());
			}

		}

		ByteBuffer bytes = source.getBytesUnsafe(columnName);
		Object value = codecFor(columnType).deserialize(bytes, ProtocolVersion.NEWEST_SUPPORTED);

		return value;
	}

}
