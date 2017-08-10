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
package net.helenus.test.integration.core.simple;


import static net.helenus.core.Query.eq;

import net.helenus.core.HelenusSession;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import net.helenus.core.Helenus;
import net.helenus.core.Operator;
import net.helenus.support.Fun;
import net.helenus.test.integration.build.AbstractEmbeddedCassandraTest;

import java.util.Optional;

public class SimpleUserTest extends AbstractEmbeddedCassandraTest {

	static User user;

	static HelenusSession session;

	@BeforeClass
	public static void beforeTest() {
		session = Helenus.init(getSession()).showCql().add(User.class).autoCreateDrop().get();
		user = Helenus.dsl(User.class, session.getMetadata());
	}

	public static class UserImpl implements User {

		Long id;
		String name;
		Integer age;
		UserType type;

		@Override
		public Long id() {
			return id;
		}

		@Override
		public String name() {
			return name;
		}

		@Override
		public Integer age() {
			return age;
		}

		@Override
		public UserType type() {
			return type;
		}

	}

	@Test
	public void testCruid() throws Exception {

		UserImpl newUser = new UserImpl();
		newUser.id = 100L;
		newUser.name = "alex";
		newUser.age = 34;
		newUser.type = UserType.USER;

		// CREATE

		session.upsert(newUser).sync();

		// READ

		// select row and map to entity

		User actual = session.selectAll(User.class).mapTo(User.class).where(user::id, eq(100L)).sync().findFirst().get();
		assertUsers(newUser, actual);

		// select as object

		actual = session.select(User.class).where(user::id, eq(100L)).sync().findFirst().get();
		assertUsers(newUser, actual);

		// select by columns

		actual = session.select()
			.column(user::id)
			.column(user::name)
			.column(user::age)
			.column(user::type)
			.mapTo(User.class)
			.where(user::id, eq(100L))
			.sync().findFirst().get();
		assertUsers(newUser, actual);

		// select by columns

		actual = session.select(User.class)
			.mapTo(User.class)
			.where(user::id, eq(100L))
			.sync().findFirst().get();
		assertUsers(newUser, actual);

		// select as object and mapTo

		actual = session.select(user::id, user::name, user::age, user::type)
			.mapTo(User.class)
			.where(user::id, eq(100L))
			.sync().findFirst().get();
		assertUsers(newUser, actual);

		// select single column

		String name = session.select(user::name)
				.where(user::id, eq(100L))
				.sync().findFirst().get()._1;

		Assert.assertEquals(newUser.name(), name);

		// select single column in array tuple

		name = (String) session.select()
				.column(user::name)
				.where(user::id, eq(100L))
				.sync().findFirst().get()._a[0];

		Assert.assertEquals(newUser.name(), name);

		// UPDATE

		session.update(user::name, "albert")
					.set(user::age, 35)
					.where(user::id, Operator.EQ, 100L).sync();

		long cnt = session.count(user).where(user::id, Operator.EQ, 100L).sync();
		Assert.assertEquals(1L, cnt);

		name = session.select(user::name)
				.where(user::id, Operator.EQ, 100L)
				.map(t -> "_" + t._1)
				.sync()
				.findFirst()
				.get();

		Assert.assertEquals("_albert", name);

		User u = session.select(User.class).where(user::id, eq(100L)).sync().findFirst().get();

		Assert.assertEquals(Long.valueOf(100L), u.id());
		Assert.assertEquals("albert", u.name());
		Assert.assertEquals(Integer.valueOf(35), u.age());

		// INSERT
		User greg = session.<User>insert()
				.value(user::name, "greg")
				.value(user::age, 44)
				.value(user::type, UserType.USER)
                .value(user::id, 1234L)
                .sync();

		session.update()
			.set(user::name, null)
			.set(user::age, null)
			.set(user::type, null)
			.where(user::id, eq(100L)).sync();


		Fun.Tuple3<String, Integer, UserType> tuple = session.select(user::name, user::age, user::type)
				.where(user::id, eq(100L)).sync().findFirst().get();

		Assert.assertNull(tuple._1);
		Assert.assertNull(tuple._2);
		Assert.assertNull(tuple._3);

		// DELETE

		session.delete(user).where(user::id, eq(100L)).sync();

		cnt = session.select().count().where(user::id, eq(100L)).sync();
		Assert.assertEquals(0L, cnt);

	}

	private void assertUsers(User expected, User actual) {
		Assert.assertEquals(expected.id(), actual.id());
		Assert.assertEquals(expected.name(), actual.name());
		Assert.assertEquals(expected.age(), actual.age());
		Assert.assertEquals(expected.type(), actual.type());
	}

}
