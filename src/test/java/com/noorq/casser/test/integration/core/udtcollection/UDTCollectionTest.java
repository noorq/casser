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
package com.noorq.casser.test.integration.core.udtcollection;

import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.CreateType;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.noorq.casser.core.Casser;
import com.noorq.casser.core.CasserSession;
import com.noorq.casser.test.integration.build.AbstractEmbeddedCassandraTest;

public class UDTCollectionTest extends AbstractEmbeddedCassandraTest {

	//static Book book = Casser.dsl(Book.class);
	
	static CasserSession csession;

	@BeforeClass
	public static void beforeTest() {
		//csession = Casser.init(getSession()).showCql().add(book).autoCreateDrop().get();
		
		/*
		Session session = getSession();
		
		
		CreateType ct = SchemaBuilder.createType("address");
		ct.addColumn("street", DataType.text());
		ct.addColumn("city", DataType.text());
		ct.addColumn("zip_code", DataType.cint());
		ct.addColumn("phones", DataType.set(DataType.text()));
		String cql = ct.build();
		
		System.out.println(cql);
		
		session.execute(cql);
		
		Create create = SchemaBuilder.createTable("users");
		
		create.addPartitionKey("id", DataType.uuid());
		create.addUDTMapColumn("addresses", DataType.text(), SchemaBuilder.frozen("address"));
		
		cql = create.buildInternal();
		
		System.out.println(cql);
		*/
		
	}
	
	@Test
	public void test() {

		//System.out.println(book);
	
	}
	
	
}
