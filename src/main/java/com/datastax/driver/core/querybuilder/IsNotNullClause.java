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
package com.datastax.driver.core.querybuilder;

import java.util.List;

import com.datastax.driver.core.CodecRegistry;

public class IsNotNullClause extends Clause {

	final String name;

	public IsNotNullClause(String name) {
		this.name = name;
	}

	@Override
	String name() {
		return name;
	}

	@Override
	Object firstValue() {
		return null;
	}

	@Override
	void appendTo(StringBuilder sb, List<Object> variables, CodecRegistry codecRegistry) {
		Utils.appendName(name, sb).append(" IS NOT NULL");
	}

	@Override
	boolean containsBindMarker() {
		return false;
	}
}
