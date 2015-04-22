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
package com.noorq.casser.test.unit.support;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.noorq.casser.support.Immutables;

public class ImmutablesTest {

	@Test
	public void testSet() {
		
		Set<Integer> set = Immutables.setOf(123);
		
		Assert.assertEquals(1, set.size());
		Assert.assertFalse(set.isEmpty());
		
		Assert.assertTrue(set.contains(123));
		Assert.assertFalse(set.contains(125));
		
		int c = 0;
		for (Integer v : set) {
			Assert.assertEquals(Integer.valueOf(123), v);
			c++;
		}
		
		Assert.assertEquals(1, c);
	}
	
	@Test
	public void testList() {
		
		List<Integer> list = Immutables.listOf(123);
		
		Assert.assertEquals(1, list.size());
		Assert.assertFalse(list.isEmpty());
		
		Assert.assertTrue(list.contains(123));
		Assert.assertFalse(list.contains(125));
		
		int c = 0;
		for (Integer v : list) {
			Assert.assertEquals(Integer.valueOf(123), v);
			c++;
		}
		
		Assert.assertEquals(1, c);
	}
	
	@Test
	public void testMap() {
		
		Map<Integer, Integer> map = Immutables.mapOf(123, 555);
		
		Assert.assertEquals(1, map.size());
		Assert.assertFalse(map.isEmpty());
		
		Assert.assertTrue(map.containsKey(123));
		Assert.assertFalse(map.containsKey(125));
		
		int c = 0;
		for (Integer v : map.keySet()) {
			Assert.assertEquals(Integer.valueOf(123), v);
			c++;
		}
		
		Assert.assertEquals(1, c);
		
	    c = 0;
		for (Integer v : map.values()) {
			Assert.assertEquals(Integer.valueOf(555), v);
			c++;
		}
		
		Assert.assertEquals(1, c);
		
		c = 0;
		for (Map.Entry<Integer, Integer> e : map.entrySet()) {
			Assert.assertEquals(Integer.valueOf(123), e.getKey());
			Assert.assertEquals(Integer.valueOf(555), e.getValue());
			c++;
		}
		
		Assert.assertEquals(1, c);
	}
	
}
