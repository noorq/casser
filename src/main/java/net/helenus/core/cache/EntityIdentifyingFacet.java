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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.helenus.mapping.HelenusProperty;

public class EntityIdentifyingFacet extends Facet {

	private final Set<HelenusProperty> properties;

	public EntityIdentifyingFacet(HelenusProperty prop) {
		properties = new HashSet<HelenusProperty>();
		properties.add(prop);
	}

	public EntityIdentifyingFacet(Set<HelenusProperty> props) {
		properties = props;
	}

	public boolean isFullyBound() {
		return false;
	}

	public Set<HelenusProperty> getProperties() {
		return properties;
	}

	public Binder binder() {
		return new Binder(properties);
	}

	public static class Binder {

		private final Set<HelenusProperty> properties = new HashSet<HelenusProperty>();
		private Map<HelenusProperty, Object> boundProperties = new HashMap<HelenusProperty, Object>();

		Binder(Set<HelenusProperty> properties) {
			this.properties.addAll(properties);
		}

		public Binder setValueForProperty(HelenusProperty prop, Object value) {
			properties.remove(prop);
			boundProperties.put(prop, value);
			return this;
		}

		public boolean isFullyBound() {
			return properties.isEmpty();
		}

		public BoundFacet bind() {
			return new BoundFacet(boundProperties);
		}
	}
}
