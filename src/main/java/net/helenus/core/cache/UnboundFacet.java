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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.helenus.core.SchemaUtil;
import net.helenus.mapping.HelenusProperty;

public class UnboundFacet extends Facet<String> {

  private final List<HelenusProperty> properties;
  private final boolean alone;
  private final boolean combined;

  public UnboundFacet(List<HelenusProperty> properties, boolean alone, boolean combined) {
    super(SchemaUtil.createPrimaryKeyPhrase(properties));
    this.properties = properties;
    this.alone = alone;
    this.combined = combined;
  }

  public UnboundFacet(List<HelenusProperty> properties) {
    this(properties, true, true);
  }

  public UnboundFacet(HelenusProperty property, boolean alone, boolean combined) {
    super(property.getPropertyName());
    properties = new ArrayList<HelenusProperty>();
    properties.add(property);
    this.alone = alone;
    this.combined = combined;
  }

  public UnboundFacet(HelenusProperty property) {
    this(property, true, true);
  }

  public List<HelenusProperty> getProperties() {
    return properties;
  }

  public Binder binder() {
    return new Binder(name(), properties, alone, combined);
  }

  public static class Binder {

    private final String name;
    private final boolean alone;
    private final boolean combined;
    private final List<HelenusProperty> properties = new ArrayList<HelenusProperty>();
    private Map<HelenusProperty, Object> boundProperties = new HashMap<HelenusProperty, Object>();

    Binder(String name, List<HelenusProperty> properties, boolean alone, boolean combined) {
      this.name = name;
      this.properties.addAll(properties);
      this.alone = alone;
      this.combined = combined;
    }

    public Binder setValueForProperty(HelenusProperty prop, Object value) {
      properties.remove(prop);
      boundProperties.put(prop, value);
      return this;
    }

    public boolean isBound() {
      return properties.isEmpty();
    }

    public BoundFacet bind() {
      BoundFacet facet = new BoundFacet(name, boundProperties);
      facet.setUniquelyIdentifyingWhenAlone(alone);
      facet.setUniquelyIdentifyingWhenCombined(combined);
      return facet;
    }
  }
}
