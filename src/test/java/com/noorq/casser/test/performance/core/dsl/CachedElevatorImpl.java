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

public final class CachedElevatorImpl implements Elevator, MapExportable {

	private final Map<String, Object> backingMap;
	
	private int mask = 0;
	
	private int height;
	private Double price;
	private String name;
	
	public CachedElevatorImpl(Map<String, Object> backingMap) {
		this.backingMap = backingMap;
	}
	
	@Override
	public int height() {
		
		if ((mask & 1) == 0) {

			Object obj = backingMap.get("height");
			if (obj != null) {
				height = ((Integer) obj).intValue();
			}
			
			mask &= 1;
		}
		return height;
	}

	@Override
	public Double price() {
		
		if ((mask & 2) == 0) {
		
			Object obj = backingMap.get("price");
			if (obj != null) {
				price = (Double) obj;
			}
		
			mask &= 2;
		}
		return price;
	}

	@Override
	public String name() {
		
		if ((mask & 4) == 0) {
		
			Object obj = backingMap.get("name");
			if (obj != null) {
				name = (String) obj;
			}
			
			mask &= 4;
		}
		return name;
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
