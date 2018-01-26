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

import java.util.Map;
import java.util.Set;
import net.helenus.core.Getter;

public interface MapExportable {
  String TO_MAP_METHOD = "toMap";
  String TO_READ_SET_METHOD = "toReadSet";
  String PUT_METHOD = "put";

  Map<String, Object> toMap();

  default Map<String, Object> toMap(boolean mutable) {
    return null;
  }

  default Set<String> toReadSet() {
    return null;
  }

  default void put(String key, Object value) {}

  default <T> void put(Getter<T> getter, T value) {}
}
