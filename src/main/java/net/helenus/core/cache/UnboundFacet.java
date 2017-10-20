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

	public UnboundFacet(List<HelenusProperty> properties) {
		super(SchemaUtil.createPrimaryKeyPhrase(properties));
		this.properties = properties;
	}

	public UnboundFacet(HelenusProperty property) {
		super(property.getPropertyName());
		properties = new ArrayList<HelenusProperty>();
		properties.add(property);
	}

	public List<HelenusProperty> getProperties() {
		return properties;
	}

	public Binder binder() {
		return new Binder(name(), properties);
	}

	public static class Binder {

		private final String name;
		private final List<HelenusProperty> properties = new ArrayList<HelenusProperty>();
		private Map<HelenusProperty, Object> boundProperties = new HashMap<HelenusProperty, Object>();

		Binder(String name, List<HelenusProperty> properties) {
			this.name = name;
			this.properties.addAll(properties);
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
			return new BoundFacet(name, boundProperties);
		}
	}
}
