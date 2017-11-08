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

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import net.helenus.core.SessionRepository;
import net.helenus.mapping.HelenusProperty;

public final class RowColumnValueProvider implements ColumnValueProvider {

  private final SessionRepository repository;

  public RowColumnValueProvider(SessionRepository repository) {
    this.repository = repository;
  }

  @Override
  public <V> V getColumnValue(Object sourceObj, int columnIndex, HelenusProperty property, boolean immutable) {

    Row source = (Row) sourceObj;

    Object value = null;
    if (columnIndex != -1) {
      value = readValueByIndex(source, columnIndex, immutable);
    } else {
      value = readValueByName(source, property.getColumnName().getName(), immutable);
    }

    if (value != null) {

      Optional<Function<Object, Object>> converter = property.getReadConverter(repository);

      if (converter.isPresent()) {
        value = converter.get().apply(value);
      }
    }

    return (V) value;
  }

  private Object readValueByIndex(Row source, int columnIndex, boolean immutable) {

    if (source.isNull(columnIndex)) {
      return null;
    }

    ColumnDefinitions columnDefinitions = source.getColumnDefinitions();

    DataType columnType = columnDefinitions.getType(columnIndex);

    if (columnType.isCollection()) {

      List<DataType> typeArguments = columnType.getTypeArguments();

      switch (columnType.getName()) {
        case SET:
          Set set = source.getSet(columnIndex, codecFor(typeArguments.get(0)).getJavaType());
          return immutable ? ImmutableSet.copyOf(set) : set;
        case MAP:
          Map map =
              source.getMap(
                  columnIndex,
                  codecFor(typeArguments.get(0)).getJavaType(),
                  codecFor(typeArguments.get(1)).getJavaType());
          return immutable ? ImmutableMap.copyOf(map) : map;
        case LIST:
          List list = source.getList(columnIndex, codecFor(typeArguments.get(0)).getJavaType());
          return immutable ? ImmutableList.copyOf(list) : list;
      }
    }

    ByteBuffer bytes = source.getBytesUnsafe(columnIndex);
    Object value = codecFor(columnType).deserialize(bytes, ProtocolVersion.NEWEST_SUPPORTED);

    return value;
  }

  private Object readValueByName(Row source, String columnName, boolean immutable) {

    if (source.isNull(columnName)) {
      return null;
    }

    ColumnDefinitions columnDefinitions = source.getColumnDefinitions();

    DataType columnType = columnDefinitions.getType(columnName);

    if (columnType.isCollection()) {

      List<DataType> typeArguments = columnType.getTypeArguments();

      switch (columnType.getName()) {
        case SET:
          Set set = source.getSet(columnName, codecFor(typeArguments.get(0)).getJavaType());
          return immutable ? ImmutableSet.copyOf(set) : set;
        case MAP:
          Map map =
              source.getMap(
                  columnName,
                  codecFor(typeArguments.get(0)).getJavaType(),
                  codecFor(typeArguments.get(1)).getJavaType());
          return immutable ? ImmutableMap.copyOf(map) : map;
        case LIST:
          List list = source.getList(columnName, codecFor(typeArguments.get(0)).getJavaType());
          return immutable ? ImmutableList.copyOf(list) : list;
      }
    }

    ByteBuffer bytes = source.getBytesUnsafe(columnName);
    Object value = codecFor(columnType).deserialize(bytes, ProtocolVersion.NEWEST_SUPPORTED);

    return value;
  }
}
