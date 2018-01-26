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
package net.helenus.core.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import net.helenus.mapping.HelenusProperty;

public class BoundFacet extends Facet<String> {
  private final Map<HelenusProperty, Object> properties;

  public BoundFacet(HelenusProperty property, Object value) {
    super(property.getPropertyName(), value == null ? null : value.toString());
    this.properties = new HashMap<HelenusProperty, Object>(1);
    this.properties.put(property, value);
  }

  public Set<HelenusProperty> getProperties() {
    return properties.keySet();
  }

  public BoundFacet(String name, Map<HelenusProperty, Object> properties) {
    super(
        name,
        (properties.keySet().size() > 1)
            ? "["
                + String.join(
                    ", ",
                    properties
                        .keySet()
                        .stream()
                        .map(key -> properties.get(key).toString())
                        .collect(Collectors.toSet()))
                + "]"
            : String.join(
                "",
                properties
                    .keySet()
                    .stream()
                    .map(key -> properties.get(key).toString())
                    .collect(Collectors.toSet())));
    this.properties = properties;
  }
}
