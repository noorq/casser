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
package net.helenus.core;

import java.util.HashMap;
import java.util.Map;

public enum Operator {
  EQ("=="),

  IN("in"),

  GT(">"),

  LT("<"),

  GTE(">="),

  LTE("<=");

  private static final Map<String, Operator> indexByName = new HashMap<String, Operator>();

  static {
    for (Operator fo : Operator.values()) {
      indexByName.put(fo.getName(), fo);
    }
  }

  private final String name;

  private Operator(String name) {
    this.name = name;
  }

  public static Operator findByOperator(String name) {
    return indexByName.get(name);
  }

  public String getName() {
    return name;
  }
}
