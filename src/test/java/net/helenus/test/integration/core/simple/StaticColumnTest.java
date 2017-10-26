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

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import net.helenus.core.Helenus;
import net.helenus.core.HelenusSession;
import net.helenus.core.Query;
import net.helenus.test.integration.build.AbstractEmbeddedCassandraTest;

public class StaticColumnTest extends AbstractEmbeddedCassandraTest {

	static HelenusSession session;
	static Message message;

	@BeforeClass
	public static void beforeTest() {
		session = Helenus.init(getSession()).showCql().addPackage(Message.class.getPackage().getName()).autoCreateDrop()
				.get();
		message = Helenus.dsl(Message.class, session.getMetadata());
	}

	@Test
	public void testPrint() {
		System.out.println(message);
	}

	@Test
	public void testCRUID() throws TimeoutException {

		MessageImpl msg = new MessageImpl();
		msg.id = 123;
		msg.timestamp = new Date();
		msg.from = "Alex";
		msg.to = "Bob";
		msg.msg = "hi";

		// CREATE

		session.insert(msg).sync();

		msg.id = 123;
		msg.to = "Craig";

		session.insert(msg).sync();

		// READ

		List<Message> actual = session.<Message>select(message).where(message::id, Query.eq(123)).sync()
				.collect(Collectors.toList());

		Assert.assertEquals(2, actual.size());

		Message toCraig = actual.stream().filter(m -> m.to().equals("Craig")).findFirst().get();
		assertMessages(msg, toCraig);

		// UPDATE

		session.update().set(message::from, "Albert").where(message::id, Query.eq(123))
				.onlyIf(message::from, Query.eq("Alex")).sync();

		long cnt = session.select(message::from).where(message::id, Query.eq(123)).sync()
				.filter(t -> t._1.equals("Albert")).count();

		Assert.assertEquals(2, cnt);

		// INSERT

		session.update().set(message::from, null).where(message::id, Query.eq(123)).sync();

		session.select(message::from).where(message::id, Query.eq(123)).sync().map(t -> t._1)
				.forEach(Assert::assertNull);

		session.update().set(message::from, "Alex").where(message::id, Query.eq(123))
				.onlyIf(message::from, Query.eq(null)).sync();

		// DELETE

		session.delete().where(message::id, Query.eq(123)).sync();

		cnt = session.count().where(message::id, Query.eq(123)).sync();
		Assert.assertEquals(0, cnt);
	}

	private void assertMessages(Message expected, Message actual) {
		Assert.assertEquals(expected.id(), actual.id());
		Assert.assertEquals(expected.from(), actual.from());
		Assert.assertEquals(expected.timestamp(), actual.timestamp());
		Assert.assertEquals(expected.to(), actual.to());
		Assert.assertEquals(expected.message(), actual.message());
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
}
