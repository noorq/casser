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
import casser.test.integration.build.AbstractEmbeddedCassandraTest;

public class SimpleUserTest extends AbstractEmbeddedCassandraTest {

	User user;
	
	CasserSession session;
	
	@Before
	public void beforeTest() {
		
		user = Casser.dsl(User.class);
		
		session = Casser.init(getSession()).showCql().create(User.class).get();
	}
	
	@Test
	public void testCruid() {
		
		User alex = Casser.pojo(User.class);
		alex.setId(123L);
		alex.setName("alex");
		alex.setAge(34);
		
		session.upsert(alex).sync();
	
		session.update(user::setName, "albert").set(user::setAge, 35)
			.where(user::getId, "==", 123L).sync();
		
		String name = session.select(user::getName)
				.where(user::getId, "==", 123L)
				.sync()
				.findFirst()
				.get()
				.v1;
		
		Assert.assertEquals("albert", name);
		
		session.delete(user).where(user::getId, "==", 123L).sync();
		
	}
	

	
}
