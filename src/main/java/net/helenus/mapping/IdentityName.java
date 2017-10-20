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
package net.helenus.mapping;

import net.helenus.support.CqlUtil;

public final class IdentityName {

	private final String name;

	private final boolean forceQuote;

	public IdentityName(String name, boolean forceQuote) {
		this.name = name.toLowerCase();
		this.forceQuote = forceQuote;
	}

	public static IdentityName of(String name, boolean forceQuote) {
		return new IdentityName(name, forceQuote);
	}

	public String getName() {
		return name;
	}

	public boolean isForceQuote() {
		return forceQuote;
	}

	public String toCql(boolean overrideForceQuote) {
		if (overrideForceQuote) {
			return CqlUtil.forceQuote(name);
		} else {
			return name;
		}
	}

	public String toCql() {
		return toCql(forceQuote);
	}

	@Override
	public String toString() {
		return toCql();
	}
}
