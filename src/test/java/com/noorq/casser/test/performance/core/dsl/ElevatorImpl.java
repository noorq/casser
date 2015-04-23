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
package com.noorq.casser.test.performance.core.dsl;

import java.util.Map;

import com.noorq.casser.core.reflect.MapExportable;

public final class ElevatorImpl implements Elevator, MapExportable {

	private final Map<String, Object> backingMap;
	
	public ElevatorImpl(Map<String, Object> backingMap) {
		this.backingMap = backingMap;
	}
	
	@Override
	public int height() {
		Object obj = backingMap.get("height");
		if (obj != null) {
			return ((Integer) obj).intValue();
		}
		return 0;
	}

	@Override
	public Double price() {
		Object obj = backingMap.get("price");
		if (obj != null) {
			return (Double) obj;
		}
		return null;
	}

	@Override
	public String name() {
		Object obj = backingMap.get("name");
		if (obj != null) {
			return (String) obj;
		}
		return null;
	}

	@Override
	public Map<String, Object> toMap() {
		return backingMap;
	}

	@Override
	public String toString() {
		return backingMap.toString();
	}
	
}
