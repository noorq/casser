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
package com.noorq.casser.core;

import java.util.Comparator;

import com.noorq.casser.mapping.CasserMappingProperty;

public enum OrdinalBasedPropertyComparator implements Comparator<CasserMappingProperty> {

	INSTANCE;

	public int compare(CasserMappingProperty o1, CasserMappingProperty o2) {

		Integer ordinal1 = o1.getOrdinal();
		Integer ordinal2 = o2.getOrdinal();

		if (ordinal1 == null) {
			if (ordinal2 == null) {
				return 0;
			}
			return -1;
		}

		if (ordinal2 == null) {
			return 1;
		}

		return ordinal1.compareTo(ordinal2);
	}

	
}
