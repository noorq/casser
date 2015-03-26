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
package casser.test.integration.core.usertype;

import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.Test;

import casser.core.Casser;
import casser.core.CasserSession;
import casser.test.integration.build.AbstractEmbeddedCassandraTest;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.CreateType;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;

public class UserDefinedTypeTest extends AbstractEmbeddedCassandraTest {

	Account account;
	
	CasserSession csession;
	
	@Before
	public void beforeTest() {
		
		account = Casser.dsl(Account.class);
		
		//csession = Casser.init(getSession()).showCql().add(Account.class).autoCreateDrop().get();
		
		Session session = getSession();
		
		
		CreateType ct = SchemaBuilder.createType("address");
		ct.addColumn("street", DataType.text());
		ct.addColumn("city", DataType.text());
		ct.addColumn("zip_code", DataType.cint());
		ct.addColumn("phones", DataType.set(DataType.text()));
		String cql = ct.build();
		
		System.out.println(cql);
		
		//session.execute("CREATE TYPE address (street text, city text, zip_code int, phones set<text>)");
		
		session.execute(cql);

		
		ct = SchemaBuilder.createType("fullname");
		ct.addColumn("firstname", DataType.text());
		ct.addColumn("lastname", DataType.text());
		cql = ct.build();
		
		System.out.println(cql);
		
		session.execute(cql);
		
		//session.execute("CREATE TYPE fullname (firstname text,lastname text)");

		System.out.println("keyspace = " + session.getLoggedKeyspace());
		
		KeyspaceMetadata km = session.getCluster().getMetadata().getKeyspace(session.getLoggedKeyspace());
		
		UserType address = km.getUserType("address");
		UserType fullname = km.getUserType("fullname");
		
		Create create = SchemaBuilder.createTable("users");
		
		create.addPartitionKey("id", DataType.uuid());
		create.addUDTColumn("name", SchemaBuilder.frozen("fullname"));
		create.addUDTMapColumn("addresses", DataType.ascii(), SchemaBuilder.frozen("address"));
		
		cql = create.buildInternal();
		
		System.out.println(cql);
		
		session.execute(create);
		
		//session.execute("CREATE TABLE users (id uuid PRIMARY KEY, name fullname, "
		//		+ "addresses map<string, address>)");
		
		
		
	}
	
	@Test
	public void testUDT() {
		
		
		Session session = getSession();
		
		Select select = QueryBuilder.select().column("\"name\".\"lastname\"").from("users");
		
		System.out.println(select);
		
		ResultSet resultSet = session.execute("SELECT \"name\".\"lastname\" FROM users;");
		
		System.out.println("resultSet = " + resultSet);
		
		
		
		//csession.select(account.getAddress()::getStreet).sync();
	}
}
