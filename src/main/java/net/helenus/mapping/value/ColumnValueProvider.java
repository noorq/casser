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

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TypeCodec;
import net.helenus.mapping.HelenusProperty;

public interface ColumnValueProvider {

  <V> V getColumnValue(Object source, int columnIndex, HelenusProperty property, boolean immutable);

  default <V> V getColumnValue(Object source, int columnIndex, HelenusProperty property) {
    return getColumnValue(source, columnIndex, property, false);
  }

  default <T> TypeCodec<T> codecFor(DataType type) {
    return CodecRegistry.DEFAULT_INSTANCE.codecFor(type);
  }
}
