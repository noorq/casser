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
package com.noorq.casser.test.integration.core.collection;

import static com.noorq.casser.core.Query.eq;
import static com.noorq.casser.core.Query.get;
import static com.noorq.casser.core.Query.getIdx;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.noorq.casser.core.Casser;
import com.noorq.casser.core.CasserSession;
import com.noorq.casser.test.integration.build.AbstractEmbeddedCassandraTest;

public class CollectionTest extends AbstractEmbeddedCassandraTest {

	Customer customer = Casser.dsl(Customer.class);
	
	CasserSession session;

	@Before
	public void beforeTest() {
		session = Casser.init(getSession()).showCql().add(customer).autoCreateDrop().get();
	}
	
	@Test
	public void testPrint() {
		System.out.println(customer);
	}
	
	@Test
	public void testSetCRUID() {
		
		UUID id = UUID.randomUUID();
		
		Set<String> aliases = new HashSet<String>();
		aliases.add("Alex");
		aliases.add("Albert");
		
		// CREATE
		
		session.insert()
		.value(customer::id, id)
		.value(customer::aliases, aliases)
		.sync();
		
		// READ
		
		// read full object
		
		Customer actual = session.select(Customer.class).where(customer::id, eq(id)).sync().findFirst().get();
		Assert.assertEquals(id, actual.id());
		Assert.assertEquals(aliases, actual.aliases());
		Assert.assertNull(actual.names());
		Assert.assertNull(actual.properties());
		
		// read full set
		
		Set<String> actualSet = session.select(customer::aliases).where(customer::id, eq(id)).sync().findFirst().get()._1;
		Assert.assertEquals(aliases, actualSet);
		
		// UPDATE
		
		Set<String> expected = new HashSet<String>();
		expected.add("unknown");
		
		session.update().set(customer::aliases, expected).where(customer::id, eq(id)).sync();
		
		actual = session.select(Customer.class).where(customer::id, eq(id)).sync().findFirst().get();
		Assert.assertEquals(id, actual.id());
		Assert.assertEquals(expected, actual.aliases());
		
		// INSERT
		
		// add operation
		
		expected.add("add");
		session.update().add(customer::aliases, "add").where(customer::id, eq(id)).sync();
		
		actualSet = session.select(customer::aliases).where(customer::id, eq(id)).sync().findFirst().get()._1;
		Assert.assertEquals(expected, actualSet);
		
		// addAll operation
		expected.addAll(aliases);
		session.update().addAll(customer::aliases, aliases).where(customer::id, eq(id)).sync();
		
		actualSet = session.select(customer::aliases).where(customer::id, eq(id)).sync().findFirst().get()._1;
		Assert.assertEquals(expected, actualSet);
		
		// DELETE
		
		// remove single value
		
		expected.remove("add");
		session.update().remove(customer::aliases, "add").where(customer::id, eq(id)).sync();
		
		actualSet = session.select(customer::aliases).where(customer::id, eq(id)).sync().findFirst().get()._1;
		Assert.assertEquals(expected, actualSet);
		
		// remove values
		
		expected.removeAll(aliases);
		session.update().removeAll(customer::aliases, aliases).where(customer::id, eq(id)).sync();
		
		actualSet = session.select(customer::aliases).where(customer::id, eq(id)).sync().findFirst().get()._1;
		Assert.assertEquals(expected, actualSet);
		
		// remove full list
		
		session.update().set(customer::aliases, null).where(customer::id, eq(id)).sync();
		
		actualSet = session.select(customer::aliases).where(customer::id, eq(id)).sync().findFirst().get()._1;
		Assert.assertNull(actualSet);
		
		// remove object
		
		session.delete().where(customer::id, eq(id)).sync();
		Long cnt = session.count().where(customer::id, eq(id)).sync();
		Assert.assertEquals(Long.valueOf(0), cnt);
		
	}
	
	
	@Test
	public void testListCRUID() {
		
		UUID id = UUID.randomUUID();
		
		List<String> names = new ArrayList<String>();
		names.add("Alex");
		names.add("Albert");
		
		// CREATE
		
		session.insert()
		.value(customer::id, id)
		.value(customer::names, names)
		.sync();
		
		// READ
		
		// read full object
		
		Customer actual = session.select(Customer.class).where(customer::id, eq(id)).sync().findFirst().get();
		Assert.assertEquals(id, actual.id());
		Assert.assertEquals(names, actual.names());
		Assert.assertNull(actual.aliases());
		Assert.assertNull(actual.properties());
		
		// read full list
		
		List<String> actualList = session.select(customer::names).where(customer::id, eq(id)).sync().findFirst().get()._1;
		Assert.assertEquals(names, actualList);
		
		// read single value by index
		
		String cql = session.select(getIdx(customer::names, 1))
				.where(customer::id, eq(id)).cql();

		System.out.println("Still not supporting cql = " + cql);
		
		// UPDATE
		
		List<String> expected = new ArrayList<String>();
		expected.add("unknown");
		
		session.update().set(customer::names, expected).where(customer::id, eq(id)).sync();
		
		actual = session.select(Customer.class).where(customer::id, eq(id)).sync().findFirst().get();
		Assert.assertEquals(id, actual.id());
		Assert.assertEquals(expected, actual.names());
		
		// INSERT
		
		// prepend operation
		
		expected.add(0, "prepend");
		session.update().prepend(customer::names, "prepend").where(customer::id, eq(id)).sync();
		
		actualList = session.select(customer::names).where(customer::id, eq(id)).sync().findFirst().get()._1;
		Assert.assertEquals(expected, actualList);
		
		// append operation
		
		expected.add("append");
		session.update().append(customer::names, "append").where(customer::id, eq(id)).sync();
		
		actualList = session.select(customer::names).where(customer::id, eq(id)).sync().findFirst().get()._1;
		Assert.assertEquals(expected, actualList);
		
		// prependAll operation
		expected.addAll(0, names);
		session.update().prependAll(customer::names, names).where(customer::id, eq(id)).sync();
		
		actualList = session.select(customer::names).where(customer::id, eq(id)).sync().findFirst().get()._1;
		Assert.assertEquals(expected, actualList);
		
		// appendAll operation
		expected.addAll(names);
		session.update().appendAll(customer::names, names).where(customer::id, eq(id)).sync();
		
		actualList = session.select(customer::names).where(customer::id, eq(id)).sync().findFirst().get()._1;
		Assert.assertEquals(expected, actualList);
		
		// set by Index
		
		expected.set(5, "inserted");
		session.update().setIdx(customer::names, 5, "inserted").where(customer::id, eq(id)).sync();
		
		actualList = session.select(customer::names).where(customer::id, eq(id)).sync().findFirst().get()._1;
		Assert.assertEquals(expected, actualList);
		
		// DELETE
		
		// remove single value
		
		expected.remove("inserted");
		session.update().discard(customer::names, "inserted").where(customer::id, eq(id)).sync();
		
		actualList = session.select(customer::names).where(customer::id, eq(id)).sync().findFirst().get()._1;
		Assert.assertEquals(expected, actualList);
		
		// remove values
		
		expected.removeAll(names);
		session.update().discardAll(customer::names, names).where(customer::id, eq(id)).sync();
		
		actualList = session.select(customer::names).where(customer::id, eq(id)).sync().findFirst().get()._1;
		Assert.assertEquals(expected, actualList);
		
		// remove full list
		
		session.update().set(customer::names, null).where(customer::id, eq(id)).sync();
		
		actualList = session.select(customer::names).where(customer::id, eq(id)).sync().findFirst().get()._1;
		Assert.assertNull(actualList);
		
		// remove object
		
		session.delete().where(customer::id, eq(id)).sync();
		Long cnt = session.count().where(customer::id, eq(id)).sync();
		Assert.assertEquals(Long.valueOf(0), cnt);
		
	}
	
	@Test
	public void testMapCRUID() {
	
		UUID id = UUID.randomUUID();
		
		Map<String, String> props = new HashMap<String, String>();
		props.put("key1", "value1");
		props.put("key2", "value2");
		
		// CREATE
		
		session.insert()
		.value(customer::id, id)
		.value(customer::properties, props)
		.sync();
		
		// READ
		
		// read full object
		
		Customer actual = session.select(Customer.class).where(customer::id, eq(id)).sync().findFirst().get();
		Assert.assertEquals(id, actual.id());
		Assert.assertEquals(props, actual.properties());
		Assert.assertNull(actual.aliases());
		Assert.assertNull(actual.names());
		
		// read full map
		
		Map<String, String> actualMap = session.select(customer::properties).where(customer::id, eq(id)).sync().findFirst().get()._1;
		Assert.assertEquals(props, actualMap);
		
		// read single key-value in map
		
		String cql = session.select(get(customer::properties, "key1"))
				.where(customer::id, eq(id)).cql();

		System.out.println("Still not supporting cql = " + cql);
		
		// UPDATE
		
		Map<String, String> expected = new HashMap<String, String>();
		expected.put("k1", "v1");
		expected.put("k2", "v2");
		
		session.update().set(customer::properties, expected).where(customer::id, eq(id)).sync();
		
		actual = session.select(Customer.class).where(customer::id, eq(id)).sync().findFirst().get();
		Assert.assertEquals(id, actual.id());
		Assert.assertEquals(expected, actual.properties());
		
		// INSERT
		
		// put operation
		
		expected.put("k3", "v3");
		session.update().put(customer::properties, "k3", "v3").where(customer::id, eq(id)).sync();
		
		actualMap = session.select(customer::properties).where(customer::id, eq(id)).sync().findFirst().get()._1;
		Assert.assertEquals(expected, actualMap);
		
		// putAll operation
		expected.putAll(props);
		session.update().putAll(customer::properties, props).where(customer::id, eq(id)).sync();
		
		actualMap = session.select(customer::properties).where(customer::id, eq(id)).sync().findFirst().get()._1;
		Assert.assertEquals(expected, actualMap);
		
		// put existing
		
		expected.put("k3", "v33");
		session.update().put(customer::properties, "k3", "v33").where(customer::id, eq(id)).sync();
		
		actualMap = session.select(customer::properties).where(customer::id, eq(id)).sync().findFirst().get()._1;
		Assert.assertEquals(expected, actualMap);
		
		// DELETE
		
		// remove single key
		
		expected.remove("k3");
		session.update().put(customer::properties, "k3", null).where(customer::id, eq(id)).sync();
		
		actualMap = session.select(customer::properties).where(customer::id, eq(id)).sync().findFirst().get()._1;
		Assert.assertEquals(expected, actualMap);
		
		// remove full map
		
		session.update().set(customer::properties, null).where(customer::id, eq(id)).sync();
		
		actualMap = session.select(customer::properties).where(customer::id, eq(id)).sync().findFirst().get()._1;
		Assert.assertNull(actualMap);
		
		// remove object
		
		session.delete().where(customer::id, eq(id)).sync();
		Long cnt = session.count().where(customer::id, eq(id)).sync();
		Assert.assertEquals(Long.valueOf(0), cnt);
		
		
	}
	
}
