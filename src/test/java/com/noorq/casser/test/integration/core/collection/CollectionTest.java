package com.noorq.casser.test.integration.core.collection;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.noorq.casser.core.Casser;
import com.noorq.casser.core.CasserSession;
import com.noorq.casser.test.integration.build.AbstractEmbeddedCassandraTest;

public class CollectionTest extends AbstractEmbeddedCassandraTest {

	Customer customer = Casser.dsl(Customer.class);
	
	CasserSession csession;

	@Before
	public void beforeTest() {
		
		Session session = getSession();
		
		Create create = SchemaBuilder.createTable("customers");
		
		create.addPartitionKey("id", DataType.uuid());
		create.addColumn("aliases", DataType.set(DataType.text()));
		create.addColumn("name", DataType.list(DataType.text()));
		create.addColumn("properties", DataType.map(DataType.text(), DataType.text()));
		
		String cql = create.buildInternal();
		
		System.out.println(cql);
		
		session.execute(create);
		
	}
	
	@Test
	public void test() {

		UUID id = UUID.randomUUID();
		
		Session session = getSession();

		Map<String, String> props = new HashMap<String, String>();
		props.put("key1", "value1");
		props.put("key2", "value2");
		
		Insert insert = QueryBuilder.insertInto("customers")
				.value("id", id)
				.value("properties", props);
		
		System.out.println(insert);
		
		session.execute(insert);

		String cql = "SELECT properties FROM customers";

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
