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
package com.noorq.casser.test.integration.core.compound;

import java.util.Date;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.noorq.casser.core.Casser;
import com.noorq.casser.core.CasserSession;
import com.noorq.casser.support.Mutable;
import com.noorq.casser.test.integration.build.AbstractEmbeddedCassandraTest;

public class CompondKeyTest extends AbstractEmbeddedCassandraTest {

	Timeline timeline = Casser.dsl(Timeline.class);
	
	CasserSession session;
	
	public static class TimelineImpl implements Timeline {

		UUID userId;
		Date timestamp;
		String text;
		
		@Override
		public UUID userId() {
			return userId;
		}

		@Override
		public Date timestamp() {
			return timestamp;
		}

		@Override
		public String text() {
			return text;
		}
		
		
		
	}
	
	@Before
	public void beforeTest() {

		session = Casser.init(getSession()).showCql().add(Timeline.class).autoCreateDrop().get();
	}
	
	@Test
	public void test() throws Exception {
		
		UUID userId = UUID.randomUUID();
		long postTime = System.currentTimeMillis() - 100000L;
		
		session.showCql(false);
		
		for (int i = 0; i != 100; ++i) {
		
			TimelineImpl post = new TimelineImpl();
			post.userId = userId;
			post.timestamp = new Date(postTime+1000L*i);
			post.text = "hello";
			
			session.upsert(post).sync();
		}
		
		session.showCql(true);
		
		final Mutable<Date> d = new Mutable<Date>(null);
		final Mutable<Integer> c = new Mutable<Integer>(0);
		
		session.select(timeline::userId, timeline::timestamp, timeline::text)
		.where(timeline::userId, "==", userId)
		.orderBy(timeline::timestamp, "desc").limit(5).sync()
		.forEach(t -> {
			
			//System.out.println(t); 
			c.set(c.get() + 1);
			
			Date cd = d.get();
			if (cd != null) {
				Assert.assertTrue(cd.after(t._2));
			}
			d.set(t._2);
			
			});
		
		Assert.assertEquals(Integer.valueOf(5), c.get());
		
	}
	
}
