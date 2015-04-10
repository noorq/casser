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
package com.noorq.casser.test.integration.core.usertype;

import static com.noorq.casser.core.Query.eq;

import java.util.Set;
import java.util.UUID;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.noorq.casser.core.Casser;
import com.noorq.casser.core.CasserSession;
import com.noorq.casser.test.integration.build.AbstractEmbeddedCassandraTest;

public class InnerUserDefinedTypeTest extends AbstractEmbeddedCassandraTest {

	static Customer customer = Casser.dsl(Customer.class);
	static AddressInformation accountInformation = Casser.dsl(AddressInformation.class);
	
	static CasserSession csession;
	
	
	@BeforeClass
	public static void beforeTest() {
		csession = Casser.init(getSession()).showCql().add(Customer.class).autoCreateDrop().get();
	}
	
	@Test
	public void testPrint() {
		System.out.println(accountInformation);
		System.out.println(customer);
	}
	
	@Test
	public void testCrud() {
		
		UUID id = UUID.randomUUID();
		
		Address a = new Address() {

			@Override
			public String street() {
				return "1 st";
			}

			@Override
			public String city() {
				return "San Jose";
			}

			@Override
			public int zip() {
				return 95131;
			}

			@Override
			public String country() {
				return "USA";
			}

			@Override
			public Set<String> phones() {
				return Sets.newHashSet("14080000000");
			}
			
		};
		
		
		AddressInformation ai = new AddressInformation() {

			@Override
			public Address address() {
				return a;
			}
			
		};
		
		csession.insert()
			.value(customer::id, id)
			.value(customer::addressInformation, ai)
			.sync();
		
		String cql = csession.update()
			.set(customer.addressInformation().address()::street, "3 st")
			.where(customer::id, eq(id)).cql();

		System.out.println("At the time when this test was written Cassandra did not support querties like this: " + cql);

		csession.update()
			.set(customer::addressInformation, ai)
			.where(customer::id, eq(id))
			.sync();

		String street = csession.select(customer.addressInformation().address()::street)
			.where(customer::id, eq(id))
			.sync()
			.findFirst()
			.get()._1;
		
		Assert.assertEquals("1 st", street);
		
		csession.delete().where(customer::id, eq(id)).sync();
	
		Long cnt = csession.count().where(customer::id, eq(id)).sync();
		
		Assert.assertEquals(Long.valueOf(0), cnt);
		
	}
	
}
