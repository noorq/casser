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
package com.noorq.casser.mapping;

import java.util.Comparator;

public enum TypeAndOrdinalColumnComparator implements Comparator<CasserProperty> {

	INSTANCE;

	public int compare(CasserProperty thisVal, CasserProperty anotherVal) {

		int c = Integer.compare(thisVal.getColumnType().ordinal(), anotherVal.getColumnType().ordinal());
		
		if (c == 0) {
			c = Integer.compare(thisVal.getOrdinal(), anotherVal.getOrdinal());
		}

		return c;
	}
	
}
