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
package com.noorq.casser.support;

import java.lang.reflect.Array;
import java.util.Objects;


public final class Requires {

	private Requires() {
	}

	public static <T> void nonNullArray(T[] arr) {
		Objects.requireNonNull(arr, "array is null");
		int len = Array.getLength(arr);
		for (int i = 0; i != len; ++i) {
			Objects.requireNonNull(Array.get(arr, i), "element " + i + " is empty in array");
		}
	}
	
}
