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
package casser.mapping;

import casser.support.CasserMappingException;

public enum OrderingDirection {

	ASC("ASC"),

	DESC("DESC");

	private final String cql;

	private OrderingDirection(String cql) {
		this.cql = cql;
	}

	public String cql() {
		return cql;
	}
	
	
	public static OrderingDirection parseString(String name) {
		
		if (ASC.cql.equalsIgnoreCase(name)) {
			return ASC;
		}

		else if (DESC.cql.equalsIgnoreCase(name)) {
			return DESC;
		}
		
		throw new CasserMappingException("invalid ordering direction name " + name);

	}
}