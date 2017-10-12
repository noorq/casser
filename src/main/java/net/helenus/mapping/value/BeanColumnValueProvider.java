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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import net.helenus.mapping.HelenusProperty;
import net.helenus.support.HelenusException;
import net.helenus.support.HelenusMappingException;

public enum BeanColumnValueProvider implements ColumnValueProvider {
  INSTANCE;

  @Override
  public <V> V getColumnValue(Object bean, int columnIndexUnused, HelenusProperty property) {

    Method getter = property.getGetterMethod();

    Object value = null;
    try {
      value = getter.invoke(bean, new Object[] {});
    } catch (InvocationTargetException e) {
      if (e.getCause() != null) {
        throw new HelenusException("getter threw an exception", e.getCause());
      }
    } catch (ReflectiveOperationException e) {
      throw new HelenusMappingException("fail to call getter " + getter, e);
    } catch (IllegalArgumentException e) {
      throw new HelenusMappingException("invalid getter " + getter, e);
    }

    return (V) value;
  }
}
