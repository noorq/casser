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
package casser.core;

import java.util.HashMap;
import java.util.Map;

public enum FilterOperator {

	EQUAL("=="),
	
	IN("in"),

	GREATER(">"),

	LESSER("<"),

	GREATER_OR_EQUAL(">="),

	LESSER_OR_EQUAL("<=");
	
	private final String name;
	
	private final static Map<String, FilterOperator> indexByName = new HashMap<String, FilterOperator>();
	
	static {
		for (FilterOperator fo : FilterOperator.values()) {
			indexByName.put(fo.getName(), fo);
		}
	}
	
	private FilterOperator(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}
	
	public static FilterOperator findByOperator(String name) {
		return indexByName.get(name);
	}
	
}
