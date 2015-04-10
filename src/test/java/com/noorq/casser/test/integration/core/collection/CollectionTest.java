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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.noorq.casser.core.Casser;
import com.noorq.casser.core.CasserSession;
import com.noorq.casser.core.Query;
import com.noorq.casser.test.integration.build.AbstractEmbeddedCassandraTest;

public class CollectionTest extends AbstractEmbeddedCassandraTest {

	Customer customer = Casser.dsl(Customer.class);
	
	CasserSession csession;

	@Before
	public void beforeTest() {
		
		
		csession = Casser.init(getSession()).showCql().add(customer).autoCreateDrop().get();
		
		/*
		Session session = getSession();
		
		Create create = SchemaBuilder.createTable("customer");
		
		create.addPartitionKey("id", DataType.uuid());
		create.addColumn("aliases", DataType.set(DataType.text()));
		create.addColumn("name", DataType.list(DataType.text()));
		create.addColumn("properties", DataType.map(DataType.text(), DataType.text()));
		
		String cql = create.buildInternal();
		
		System.out.println(cql);
		
		session.execute(create);
		*/
		
	}
	
	@Test
	public void test() {

		System.out.println(customer);
		
		UUID id = UUID.randomUUID();
		Map<String, String> props = new HashMap<String, String>();
		props.put("key1", "value1");
		props.put("key2", "value2");
		
		csession.upsert()
		.value(customer::id, id)
		.value(customer::properties, props)
		.sync();

		Update update = QueryBuilder.update("customer");
		update.with(QueryBuilder.put("properties", "key3", "value3"));
		update.where(QueryBuilder.eq("id", id));
		String cql = update.setForceNoValues(true).getQueryString();
		
		System.out.println(cql);
		
		getSession().execute(update);
		
		
		cql = csession.update().put(customer::properties, "key3", "value3")
				.where(customer::id, Query.eq(id))
				.cql();
		
		System.out.println("cql = " + cql);
		
		csession.select(Customer.class).sync().forEach(System.out::println);

		
		
		
		Session session = getSession();
		/*
		Insert insert = QueryBuilder.insertInto("customer")
				.value("id", id)
				.value("properties", props);
		
		System.out.println(insert);
		
		session.execute(insert);
		*/

		cql = "SELECT properties FROM customer";

		ResultSet resultSet = session.execute(cql);
		
		for (Row row : resultSet) {
			
			System.out.println("row = " + row);
			
		}
		
		
		//csession.select(CMap.get(customer::properties, "123")).sync();
		
		
		/*
		
		csession.select(customer::id).sync();

		// set full
		
		//csession.insert(customer::aliases, new HashSet<String>());
		csession.select(customer::aliases).sync();
		
		// set add/remove
		csession.update(customer::aliases, "+", "value").sync();
		csession.update(customer::aliases, "-", "value").sync();
		
		// list add/remove
		csession.update(customer::name, "+", "value").sync();
		csession.update("value", "+", customer::name).sync();
		
		csession.update(customer::name, 1, "value").sync();
		csession.update(customer::name, 2, "value").sync();
		csession.delete(customer::name, 2).sync();

		// map
		csession.update(customer::properties, "key", "value").sync();
		
		
		
		csession.select( 
				() -> customer.properties().get("abc"),
				() -> customer.name().get(4),

				customer.aliases().add("123")::toString,
				
				customer.properties().get("abc")::toString,
				customer.properties().put("abc", "value")::toString

				).sync();
		*/
	}
	
}
