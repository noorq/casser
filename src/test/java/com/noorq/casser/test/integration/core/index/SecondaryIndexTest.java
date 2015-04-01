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
package com.noorq.casser.test.integration.core.index;

import static com.noorq.casser.core.Query.eq;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.noorq.casser.core.Casser;
import com.noorq.casser.core.CasserSession;
import com.noorq.casser.test.integration.build.AbstractEmbeddedCassandraTest;

public class SecondaryIndexTest extends AbstractEmbeddedCassandraTest {

	Book book = Casser.dsl(Book.class);
	
	CasserSession session;
	
	@Before
	public void beforeTest() {

		session = Casser.init(getSession()).showCql().add(Book.class).autoCreateDrop().get();
	}
	
	@Test
	public void test() throws Exception {
		
		session.insert()
			.value(book::id, 123L)
			.value(book::isbn, "ABC")
			.value(book::author, "Alex")
			.sync();
		
		long actualId = session.select(book::id).where(book::isbn, eq("ABC")).sync().findFirst().get()._1;
		
		Assert.assertEquals(123L, actualId);
		
	}
	
}
