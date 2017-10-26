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
package net.helenus.test.integration.core.usertype;

import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;

import net.helenus.core.Helenus;
import net.helenus.core.HelenusSession;
import net.helenus.core.Query;
import net.helenus.test.integration.build.AbstractEmbeddedCassandraTest;

public class UserDefinedTypeTest extends AbstractEmbeddedCassandraTest {

	static Address address;
	static Account account;

	static HelenusSession session;

	@BeforeClass
	public static void beforeTest() {
		session = Helenus.init(getSession()).showCql().add(Account.class).autoCreateDrop().get();
		address = Helenus.dsl(Address.class);
		account = Helenus.dsl(Account.class);
	}

	@Test
	public void testPrint() {
		System.out.println(address);
		System.out.println(account);
	}

	@Test
	public void testMappingCRUID() throws TimeoutException {

		AddressImpl addr = new AddressImpl();
		addr.street = "1 st";
		addr.city = "San Jose";

		AccountImpl acc = new AccountImpl();
		acc.id = 123L;
		acc.address = addr;

		// CREATE

		session.upsert(acc).sync();

		// READ

		String streetName = session.select(account.address()::street).where(account::id, Query.eq(123L)).sync()
				.findFirst().get()._1;

		Assert.assertEquals("1 st", streetName);

		// UPDATE

		AddressImpl expected = new AddressImpl();
		expected.street = "2 st";
		expected.city = "San Francisco";

		session.update().set(account::address, expected).where(account::id, Query.eq(123L)).sync();

		Address actual = session.select(account::address).where(account::id, Query.eq(123L)).sync().findFirst()
				.get()._1;

		Assert.assertEquals(expected.street(), actual.street());
		Assert.assertEquals(expected.city(), actual.city());
		Assert.assertNull(actual.country());
		Assert.assertEquals(0, actual.zip());

		// INSERT using UPDATE
		session.update().set(account::address, null).where(account::id, Query.eq(123L)).sync();

		Address adrNull = session.select(account::address).where(account::id, Query.eq(123L)).sync().findFirst()
				.get()._1;
		Assert.assertNull(adrNull);

		// DELETE

		session.delete().where(account::id, Query.eq(123L)).sync();

		Long cnt = session.count().where(account::id, Query.eq(123L)).sync();
		Assert.assertEquals(Long.valueOf(0), cnt);
	}

	@Test
	public void testNoMapping() throws TimeoutException {

		String ks = getSession().getLoggedKeyspace();
		UserType addressType = getSession().getCluster().getMetadata().getKeyspace(ks).getUserType("address");

		UDTValue addressNoMapping = addressType.newValue();
		addressNoMapping.setString("line_1", "1st street");
		addressNoMapping.setString("city", "San Jose");

		AccountImpl acc = new AccountImpl();
		acc.id = 777L;
		acc.addressNoMapping = addressNoMapping;

		// CREATE

		session.upsert(acc).sync();

		// READ

		UDTValue found = session.select(account::addressNoMapping).where(account::id, Query.eq(777L)).sync().findFirst()
				.get()._1;

		Assert.assertEquals(addressNoMapping.getType(), found.getType());
		Assert.assertEquals(addressNoMapping.getString("line_1"), found.getString("line_1"));
		Assert.assertEquals(addressNoMapping.getString("city"), found.getString("city"));

		// UPDATE

		addressNoMapping = addressType.newValue();
		addressNoMapping.setString("line_1", "Market street");
		addressNoMapping.setString("city", "San Francisco");

		session.update().set(account::addressNoMapping, addressNoMapping).where(account::id, Query.eq(777L)).sync();

		found = session.select(account::addressNoMapping).where(account::id, Query.eq(777L)).sync().findFirst()
				.get()._1;

		Assert.assertEquals(addressNoMapping.getType(), found.getType());
		Assert.assertEquals(addressNoMapping.getString("line_1"), found.getString("line_1"));
		Assert.assertEquals(addressNoMapping.getString("city"), found.getString("city"));

		// INSERT using UPDATE
		session.update().set(account::addressNoMapping, null).where(account::id, Query.eq(777L)).sync();

		found = session.select(account::addressNoMapping).where(account::id, Query.eq(777L)).sync().findFirst()
				.get()._1;
		Assert.assertNull(found);

		// DELETE

		session.delete().where(account::id, Query.eq(777L)).sync();

		Long cnt = session.count().where(account::id, Query.eq(777L)).sync();
		Assert.assertEquals(Long.valueOf(0), cnt);
	}

	public static class AccountImpl implements Account {

		long id;
		Address address;
		UDTValue addressNoMapping;

		@Override
		public long id() {
			return id;
		}

		@Override
		public Address address() {
			return address;
		}

		@Override
		public UDTValue addressNoMapping() {
			return addressNoMapping;
		}
	}

	public static class AddressImpl implements Address {

		String street;
		String city;
		int zip;
		String country;
		Set<String> phones;

		@Override
		public String street() {
			return street;
		}

		@Override
		public String city() {
			return city;
		}

		@Override
		public int zip() {
			return zip;
		}

		@Override
		public String country() {
			return country;
		}

		@Override
		public Set<String> phones() {
			return phones;
		}
	}
}
