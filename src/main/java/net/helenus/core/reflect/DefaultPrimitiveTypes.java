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
package net.helenus.core.reflect;

import java.util.HashMap;
import java.util.Map;

public enum DefaultPrimitiveTypes {
  BOOLEAN(boolean.class, false),
  BYTE(byte.class, (byte) 0x0),
  CHAR(char.class, (char) 0x0),
  SHORT(short.class, (short) 0),
  INT(int.class, 0),
  LONG(long.class, 0L),
  FLOAT(float.class, 0.0f),
  DOUBLE(double.class, 0.0);

  private final Class<?> primitiveClass;
  private final Object defaultValue;

  private static final Map<Class<?>, DefaultPrimitiveTypes> map =
      new HashMap<Class<?>, DefaultPrimitiveTypes>();

  static {
    for (DefaultPrimitiveTypes type : DefaultPrimitiveTypes.values()) {
      map.put(type.getPrimitiveClass(), type);
    }
  }

  private DefaultPrimitiveTypes(Class<?> primitiveClass, Object defaultValue) {
    this.primitiveClass = primitiveClass;
    this.defaultValue = defaultValue;
  }

  public static DefaultPrimitiveTypes lookup(Class<?> primitiveClass) {
    return map.get(primitiveClass);
  }

  public Class<?> getPrimitiveClass() {
    return primitiveClass;
  }

  public Object getDefaultValue() {
    return defaultValue;
  }
}
