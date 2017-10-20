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

/**
 * An Entity is identifiable via one or more Facets
 *
 * A Facet is is a set of Properties and bound Facets
 *
 * An Entity will have it's Keyspace, Table and Schema Version Facets bound.
 *
 * A property may also have a TTL or write time bound.
 *
 * The cache contains key->value mappings of merkel-hash -> Entity or
 * Set<Entity> The only way a Set<Entity> is put into the cache is with a key =
 * hash([Entity's bound Facets, hash(filter clause from SELECT)])
 *
 * REMEMBER to update the cache on build() for all impacted facets, delete
 * existing keys and add new keys
 */
public class Facet<T> {
	private final String name;
	private T value;

	public Facet(String name) {
		this.name = name;
	}

	public Facet(String name, T value) {
		this.name = name;
		this.value = value;
	}

	public String name() {
		return name;
	}

	public T value() {
		return value;
	}

}
