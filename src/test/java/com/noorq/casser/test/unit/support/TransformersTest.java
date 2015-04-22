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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.junit.Assert;
import org.junit.Test;

import com.noorq.casser.support.Transformers;

public class TransformersTest {

	@Test
	public void testList() {
		
		List<Integer> source = new ArrayList<Integer>();
		source.add(555);
		source.add(777);
		source.add(999);
		
		List<String> out = Transformers.transformList(source, x -> "_" + x);

		Assert.assertEquals(3, out.size());
		Assert.assertEquals(false, out.isEmpty());
		Assert.assertEquals("_555", out.get(0));
		Assert.assertEquals("_777", out.get(1));
		Assert.assertEquals("_999", out.get(2));
	
		Iterator<String> i = out.iterator();
		Assert.assertTrue(i.hasNext());
		Assert.assertEquals("_555", i.next());
		Assert.assertTrue(i.hasNext());
		Assert.assertEquals("_777", i.next());
		Assert.assertTrue(i.hasNext());
		Assert.assertEquals("_999", i.next());
		Assert.assertFalse(i.hasNext());
		
		ListIterator<String> li = out.listIterator();
		Assert.assertTrue(li.hasNext());
		Assert.assertEquals(0, li.nextIndex());
		Assert.assertEquals(-1, li.previousIndex());
		Assert.assertEquals("_555", li.next());
		Assert.assertTrue(li.hasNext());
		Assert.assertEquals(1, li.nextIndex());
		Assert.assertEquals(0, li.previousIndex());
		Assert.assertEquals("_777", li.next());
		Assert.assertTrue(li.hasNext());
		Assert.assertEquals(2, li.nextIndex());
		Assert.assertEquals(1, li.previousIndex());
		Assert.assertEquals("_999", li.next());
		Assert.assertFalse(li.hasNext());
		Assert.assertEquals(3, li.nextIndex());
		Assert.assertEquals(2, li.previousIndex());
		
		
	}
	
	@Test
	public void testNullsInList() {
		
		List<Integer> source = new ArrayList<Integer>();
		source.add(555);
		source.add(null);
		source.add(999);
		
		List<String> out = Transformers.transformList(source, x -> "_" + x);
		
		Assert.assertEquals(3, out.size());
		Assert.assertEquals("_null", out.get(1));
		
	}
	
}
