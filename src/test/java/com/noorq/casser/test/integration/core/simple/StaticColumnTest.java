/*
 *      Copyright (C) 2015 The Casser Authors
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
package com.noorq.casser.test.integration.core.simple;

import static com.noorq.casser.core.Query.eq;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.noorq.casser.core.Casser;
import com.noorq.casser.test.integration.build.AbstractEmbeddedCassandraTest;

public class StaticColumnTest extends AbstractEmbeddedCassandraTest {

	static Message message = Casser.dsl(Message.class);
	
	@BeforeClass
	public static void beforeTest() {
		Casser.init(getSession()).showCql().addPackage(Message.class.getPackage().getName()).autoCreateDrop().singleton();
	}
	
	@Test
	public void testPrint() {
		System.out.println(message);
	}
	
	private static class MessageImpl implements Message {
		
		int id;
		Date timestamp;
		String from;
		String to;
		String msg;
		
		@Override
		public int id() {
			return id;
		}

		@Override
		public Date timestamp() {
			return timestamp;
		}

		@Override
		public String from() {
			return from;
		}

		@Override
		public String to() {
			return to;
		}

		@Override
		public String message() {
			return msg;
		}
		
	}
	
	
	@Test
	public void testCRUID() {
		
		MessageImpl msg = new MessageImpl();
		msg.id = 123;
		msg.timestamp = new Date();
		msg.from = "Alex";
		msg.to = "Bob";
		msg.msg = "hi";
		
		// CREATE
		
		Casser.session().insert(msg).sync();
		
		msg.id = 123;
		msg.to = "Craig";

		Casser.session().insert(msg).sync();
		
		
		// READ
		
		List<Message> actual = Casser.session().select(Message.class)
				.where(message::id, eq(123)).sync()
				.collect(Collectors.toList());
		
		Assert.assertEquals(2, actual.size());
		
		Message toCraig = actual.stream().filter(m -> m.to().equals("Craig")).findFirst().get();		
		assertMessages(msg, toCraig);
		
		// UPDATE
		
		Casser.session().update().set(message::from, "Albert")
		.where(message::id, eq(123))
		.onlyIf(message::from, eq("Alex"))
		.sync();
		
		long cnt = Casser.session().select(message::from)
				.where(message::id, eq(123)).sync()
				.filter(t -> t._1.equals("Albert"))
				.count();
		
		Assert.assertEquals(2, cnt);

		// INSERT
		
		Casser.session().update().set(message::from, null)
			.where(message::id, eq(123))
			.sync();
		
		Casser.session().select(message::from)
				.where(message::id, eq(123))
				.sync()
				.map(t -> t._1)
				.forEach(Assert::assertNull);

		Casser.session().update().set(message::from, "Alex")
			.where(message::id, eq(123))
			.onlyIf(message::from, eq(null)).sync();
		
		// DELETE
		
		Casser.session().delete().where(message::id, eq(123)).sync();
		
		cnt = Casser.session().count().where(message::id, eq(123)).sync();
		Assert.assertEquals(0, cnt);
	}
	
	private void assertMessages(Message expected, Message actual) {
		Assert.assertEquals(expected.id(), actual.id());
		Assert.assertEquals(expected.from(), actual.from());
		Assert.assertEquals(expected.timestamp(), actual.timestamp());
		Assert.assertEquals(expected.to(), actual.to());
		Assert.assertEquals(expected.message(), actual.message());
	}
	
}
