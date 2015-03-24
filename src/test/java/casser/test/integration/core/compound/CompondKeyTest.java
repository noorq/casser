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
package casser.test.integration.core.compound;

import java.util.Date;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import casser.core.Casser;
import casser.core.CasserSession;
import casser.support.Mutable;
import casser.test.integration.build.AbstractEmbeddedCassandraTest;

public class CompondKeyTest extends AbstractEmbeddedCassandraTest {

	Timeline timeline;
	
	CasserSession session;
	
	@Before
	public void beforeTest() {
		
		timeline = Casser.dsl(Timeline.class);
		
		session = Casser.init(getSession()).showCql().createDrop(Timeline.class).get();
	}
	
	@Test
	public void test() throws Exception {
		
		UUID userId = UUID.randomUUID();
		long postTime = System.currentTimeMillis() - 100000L;
		
		Timeline post = Casser.pojo(Timeline.class);
		
		session.showCql(false);
		
		for (int i = 0; i != 100; ++i) {
		
			post.setUserId(userId);
			post.setTimestamp(new Date(postTime+1000L*i));
			post.setText("hello");
			
			session.upsert(post).sync();
		}
		
		session.showCql(true);
		
		final Mutable<Date> d = new Mutable<Date>(null);
		final Mutable<Integer> c = new Mutable<Integer>(0);
		
		session.select(timeline::getUserId, timeline::getTimestamp, timeline::getText)
		.where(timeline::getUserId, "==", userId)
		.orderBy(timeline::getTimestamp, "desc").limit(5).sync()
		.forEach(t -> {
			//System.out.println(t); 
			c.set(c.get() + 1);
			
			Date cd = d.get();
			if (cd != null) {
				Assert.assertTrue(cd.after(t.v2));
			}
			d.set(t.v2);
			
			});
		
		Assert.assertEquals(Integer.valueOf(5), c.get());
		
	}
	
}
