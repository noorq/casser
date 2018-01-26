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

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.querybuilder.BindMarker;
import java.util.Optional;
import java.util.function.Function;
import net.helenus.core.HelenusValidator;
import net.helenus.core.SessionRepository;
import net.helenus.mapping.HelenusProperty;

public final class UDTColumnValuePreparer implements ColumnValuePreparer {

  private final UserType userType;
  private final SessionRepository repository;

  public UDTColumnValuePreparer(UserType userType, SessionRepository repository) {
    this.userType = userType;
    this.repository = repository;
  }

  @Override
  public Object prepareColumnValue(Object value, HelenusProperty prop) {

    if (value instanceof BindMarker) {
      return value;
    }

    HelenusValidator.INSTANCE.validate(prop, value);

    if (value != null) {

      Optional<Function<Object, Object>> converter = prop.getWriteConverter(repository);

      if (converter.isPresent()) {
        value = converter.get().apply(value);
      }

      DataType dataType = userType.getFieldType(prop.getColumnName().getName());

      return codecFor(dataType).serialize(value, ProtocolVersion.NEWEST_SUPPORTED);
    }

    return null;
  }
}
