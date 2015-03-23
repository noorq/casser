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

public enum FilterOperation {

	EQUAL("=="),
	
	IN("in"),

	GREATER(">"),

	LESSER("<"),

	GREATER_OR_EQUAL(">="),

	LESSER_OR_EQUAL("<=");
	
	private final String operator;
	
	private final static Map<String, FilterOperation> indexByOperator = new HashMap<String, FilterOperation>();
	
	static {
		for (FilterOperation fo : FilterOperation.values()) {
			indexByOperator.put(fo.getOperator(), fo);
		}
	}
	
	private FilterOperation(String operator) {
		this.operator = operator;
	}

	public String getOperator() {
		return operator;
	}
	
	public static FilterOperation findByOperator(String operator) {
		return indexByOperator.get(operator);
	}
	
}
