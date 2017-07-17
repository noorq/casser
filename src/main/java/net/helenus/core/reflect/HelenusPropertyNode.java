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

import java.util.*;
import java.util.stream.Collectors;

import net.helenus.mapping.HelenusEntity;
import net.helenus.mapping.HelenusProperty;

public final class HelenusPropertyNode implements Iterable<HelenusProperty> {

	private final HelenusProperty prop;
	private final Optional<HelenusPropertyNode> next;

	public HelenusPropertyNode(HelenusProperty prop, Optional<HelenusPropertyNode> next) {
		this.prop = prop;
		this.next = next;
	}

	public String getColumnName() {
		if (next.isPresent()) {

			List<String> columnNames = new ArrayList<String>();
			for (HelenusProperty p : this) {
				columnNames.add(p.getColumnName().toCql(true));
			}
			Collections.reverse(columnNames);

			if (prop instanceof HelenusNamedProperty) {
				int size = columnNames.size();
				StringBuilder str = new StringBuilder();
				for (int i = 0; i != size - 1; ++i) {
					if (str.length() != 0) {
						str.append(".");
					}
					str.append(columnNames.get(i));
				}
				str.append("[").append(columnNames.get(size - 1)).append("]");
				return str.toString();
			} else {
				return columnNames.stream().collect(Collectors.joining("."));
			}
		} else {
			return prop.getColumnName().toCql();
		}
	}

	public HelenusEntity getEntity() {
		if (next.isPresent()) {
			HelenusProperty last = prop;
			for (HelenusProperty p : this) {
				last = p;
			}
			return last.getEntity();
		} else {
			return prop.getEntity();
		}
	}

	public HelenusProperty getProperty() {
		return prop;
	}

	public Optional<HelenusPropertyNode> getNext() {
		return next;
	}

	public Iterator<HelenusProperty> iterator() {
		return new PropertyNodeIterator(Optional.of(this));
	}

	private static class PropertyNodeIterator implements Iterator<HelenusProperty> {

		private Optional<HelenusPropertyNode> next;

		public PropertyNodeIterator(Optional<HelenusPropertyNode> next) {
			this.next = next;
		}

		@Override
		public boolean hasNext() {
			return next.isPresent();
		}

		@Override
		public HelenusProperty next() {
			HelenusPropertyNode node = next.get();
			next = node.next;
			return node.prop;
		}

	}

}
