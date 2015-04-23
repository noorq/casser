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
package com.noorq.casser.test.integration.core.tuplecollection;

import static com.noorq.casser.core.Query.eq;
import static com.noorq.casser.core.Query.get;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class TupleKeyMapTest extends TupleCollectionTest {
	
	@Test
	public void testKeyMapCRUID() {
	
		int id = 888;
		
		Map<Section, String> notes = new HashMap<Section, String>();
		notes.put(new SectionImpl("first", 1), "value1");
		notes.put(new SectionImpl("second", 2), "value2");
		
		// CREATE
		
		session.insert()
		.value(book::id, id)
		.value(book::notes, notes)
		.sync();
		
		// READ
		
		// read full object
		
		Book actual = session.select(Book.class).where(book::id, eq(id)).sync().findFirst().get();
		Assert.assertEquals(id, actual.id());
		assertEqualMaps(notes, actual.notes());
		Assert.assertNull(actual.reviewers());
		Assert.assertNull(actual.writers());
		Assert.assertNull(actual.contents());
		
		// read full map
		
		Map<Section, String> actualMap = session.select(book::notes).where(book::id, eq(id)).sync().findFirst().get()._1;
		assertEqualMaps(notes, actualMap);
		
		// read single key-value in map
		
		String cql = session.select(get(book::notes, new SectionImpl("first", 1)))
				.where(book::id, eq(id)).cql();

		System.out.println("Still not supporting cql = " + cql);
		
		// UPDATE
		
		Map<Section, String> expected = new HashMap<Section, String>();
		expected.put(new SectionImpl("f", 1), "v1");
		expected.put(new SectionImpl("s", 1), "v2");
		
		session.update().set(book::notes, expected).where(book::id, eq(id)).sync();
		
		actual = session.select(Book.class).where(book::id, eq(id)).sync().findFirst().get();
		Assert.assertEquals(id, actual.id());
		assertEqualMaps(expected, actual.notes());
		
		// INSERT
		
		// put operation
		
		Section third = new SectionImpl("t", 3);
		
		expected.put(third, "v3");
		session.update().put(book::notes, third, "v3").where(book::id, eq(id)).sync();
		
		actualMap = session.select(book::notes).where(book::id, eq(id)).sync().findFirst().get()._1;
		assertEqualMaps(expected, actualMap);
		
		// putAll operation
		expected.putAll(notes);
		session.update().putAll(book::notes, notes).where(book::id, eq(id)).sync();
		
		actualMap = session.select(book::notes).where(book::id, eq(id)).sync().findFirst().get()._1;
		assertEqualMaps(expected, actualMap);
		
		// put existing
		
		expected.put(third, "v33");
		session.update().put(book::notes, third, "v33").where(book::id, eq(id)).sync();
		
		actualMap = session.select(book::notes).where(book::id, eq(id)).sync().findFirst().get()._1;
		assertEqualMaps(expected, actualMap);
		
		// DELETE
		
		// remove single key
		
		expected.remove(third);
		session.update().put(book::notes, third, null).where(book::id, eq(id)).sync();
		
		actualMap = session.select(book::notes).where(book::id, eq(id)).sync().findFirst().get()._1;
		assertEqualMaps(expected, actualMap);
		
		// remove full map
		
		session.update().set(book::notes, null).where(book::id, eq(id)).sync();
		
		actualMap = session.select(book::notes).where(book::id, eq(id)).sync().findFirst().get()._1;
		Assert.assertNull(actualMap);
		
		// remove object
		
		session.delete().where(book::id, eq(id)).sync();
		Long cnt = session.count().where(book::id, eq(id)).sync();
		Assert.assertEquals(Long.valueOf(0), cnt);
		
	}
	
	private void assertEqualMaps(Map<Section, String> expected, Map<Section, String> actual) {
		
		Assert.assertEquals(expected.size(), actual.size());

		for (Section e : expected.keySet()) {
			Section a = actual.keySet().stream().filter(p -> p.title().equals(e.title())).findFirst().get();
			Assert.assertEquals(e.title(), a.title());
			Assert.assertEquals(e.page(), a.page());
			Assert.assertEquals(expected.get(e), actual.get(a));
		}
		
	}
	
}


