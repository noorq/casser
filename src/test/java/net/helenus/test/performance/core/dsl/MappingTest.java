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
package net.helenus.test.performance.core.dsl;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import net.helenus.core.Helenus;

public class MappingTest {

	static Map<String, Object> fixture;

	static {
		fixture = new HashMap<String, Object>();
		fixture.put("height", Integer.valueOf(55));
		fixture.put("price", Double.valueOf(44.99));
		fixture.put("name", "first");
	}

	@Test
	public void testReflectionConstructor() {

		long t0 = System.currentTimeMillis();

		for (int i = 0; i != 100000; ++i) {
			Helenus.map(Elevator.class, fixture);
		}

		long t1 = System.currentTimeMillis() - t0;

		System.out.println("ReflectionConstructor = " + t1);
	}

	@Test
	public void testReflectionAccess() {

		long t0 = System.currentTimeMillis();

		Elevator elevator = Helenus.map(Elevator.class, fixture);

		for (int i = 0; i != 100000; ++i) {
			elevator.height();
			elevator.price();
			elevator.name();
		}

		long t1 = System.currentTimeMillis() - t0;

		System.out.println("ReflectionAccess = " + t1);
	}

	@Test
	public void testJavaAccess() {

		long t0 = System.currentTimeMillis();

		Elevator elevator = new ElevatorImpl(fixture);

		for (int i = 0; i != 100000; ++i) {
			elevator.height();
			elevator.price();
			elevator.name();
		}

		long t1 = System.currentTimeMillis() - t0;

		System.out.println("JavaAccess = " + t1);
	}

	@Test
	public void testJavaCachedAccess() {

		long t0 = System.currentTimeMillis();

		Elevator elevator = new CachedElevatorImpl(fixture);

		for (int i = 0; i != 100000; ++i) {
			elevator.height();
			elevator.price();
			elevator.name();
		}

		long t1 = System.currentTimeMillis() - t0;

		System.out.println("JavaCachedAccess = " + t1);
	}

	@Test
	public void testJavaConstructor() {

		long t0 = System.currentTimeMillis();

		for (int i = 0; i != 100000; ++i) {
			new ElevatorImpl(fixture);
		}

		long t1 = System.currentTimeMillis() - t0;

		System.out.println("JavaConstructor = " + t1);
	}
}
