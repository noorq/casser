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
package net.helenus.mapping.convert;

import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import java.nio.ByteBuffer;
import java.util.function.Function;
import net.helenus.core.SessionRepository;
import net.helenus.mapping.HelenusProperty;
import net.helenus.mapping.value.UDTColumnValuePreparer;

public class UDTValueWriter extends AbstractEntityValueWriter<UDTValue>
    implements Function<Object, UDTValue> {

  final UserType userType;
  final UDTColumnValuePreparer valuePreparer;

  public UDTValueWriter(Class<?> iface, UserType userType, SessionRepository repository) {
    super(iface);

    this.userType = userType;
    this.valuePreparer = new UDTColumnValuePreparer(userType, repository);
  }

  @Override
  void writeColumn(UDTValue udtValue, Object value, HelenusProperty prop) {

    ByteBuffer bytes = (ByteBuffer) valuePreparer.prepareColumnValue(value, prop);

    if (bytes != null) {
      udtValue.setBytesUnsafe(prop.getColumnName().getName(), bytes);
    }
  }

  @Override
  public UDTValue apply(Object source) {
    if (source != null) {
      UDTValue outValue = userType.newValue();
      write(outValue, source);
      return outValue;
    }
    return null;
  }
}
