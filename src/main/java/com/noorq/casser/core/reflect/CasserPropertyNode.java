/*
 *      Copyright (C) 2015 Noorq, Inc.
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
package com.noorq.casser.core.reflect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.noorq.casser.mapping.CasserEntity;
import com.noorq.casser.mapping.CasserProperty;

public final class CasserPropertyNode implements Iterable<CasserProperty> {

	private final CasserProperty prop;
	private final Optional<CasserPropertyNode> next;
	
	public CasserPropertyNode(CasserProperty prop, Optional<CasserPropertyNode> next) {
		this.prop = prop;
		this.next = next;
	}

	public String getColumnName() {
		if (next.isPresent()) {

			List<String> columnNames = new ArrayList<String>();
			for (CasserProperty p : this) {
				columnNames.add(p.getColumnName().toCql(true));
			}
			Collections.reverse(columnNames);
			
			if (prop instanceof CasserNamedProperty) {
				int size = columnNames.size();
				StringBuilder str = new StringBuilder();
				for (int i = 0; i != size -1; ++i) {
					if (str.length() != 0) {
						str.append(".");
					}
					str.append(columnNames.get(i));
				}
				str.append("[").append(columnNames.get(size-1)).append("]");
				return str.toString();
			}
			else {
				return columnNames.stream().collect(Collectors.joining("."));
			}
		}
		else {
			return prop.getColumnName().toCql();
		}
	}
	
	public CasserEntity getEntity() {
		if (next.isPresent()) {
			CasserProperty last = prop;
			for (CasserProperty p : this) {
				last = p;
			}
			return last.getEntity();
		}
		else {
			return prop.getEntity();
		}
	}
	
	public CasserProperty getProperty() {
		return prop;
	}

	public Optional<CasserPropertyNode> getNext() {
		return next;
	}
	
	public Iterator<CasserProperty> iterator() {
		return new PropertyNodeIterator(Optional.of(this));
	}

	private static class PropertyNodeIterator implements Iterator<CasserProperty> {

		private Optional<CasserPropertyNode> next;
		
		public PropertyNodeIterator(Optional<CasserPropertyNode> next) {
			this.next = next;
		}
		
		@Override
		public boolean hasNext() {
			return next.isPresent();
		}

		@Override
		public CasserProperty next() {
			CasserPropertyNode node = next.get();
			next = node.next;
			return node.prop;
		}
		
	}
	
}
