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
package casser.test.integration.core.simple;


import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import casser.core.Casser;
import casser.core.CasserSession;
import casser.core.Operator;
import casser.test.integration.build.AbstractEmbeddedCassandraTest;

public class SimpleUserTest extends AbstractEmbeddedCassandraTest {

	User user = Casser.dsl(User.class);
	
	CasserSession session;
	
	@Before
	public void beforeTest() {
		
		session = Casser.init(getSession()).showCql().add(User.class).autoCreateDrop().get();
	}
	
	public static class UserImpl implements User {
		
		Long id;
		String name;
		Integer age;
		
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
		
	}
	
	@Test
	public void testCruid() {
		
		UserImpl newUser = new UserImpl();
		newUser.id = 100L;
		newUser.name = "alex";
		newUser.age = 34;
		
		session.upsert(newUser).sync();
	
		session.update(user::name, "albert").set(user::age, 35)
			.where(user::id, "==", 123L).sync();
		
		long cnt = session.count(user).where(user::id, Operator.EQ, 123L).sync();
		Assert.assertEquals(1L, cnt);

		String name = session.select(user::name)
				.where(user::id, "==", 123L)
				.map(t -> "_" + t.v1)
				.sync()
				.findFirst()
				.get();
		
		Assert.assertEquals("_albert", name);
		
		session.delete(user).where(user::id, "==", 123L).sync();
		
	}
	

	
}
