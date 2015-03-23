package casser.test.integration.core.compound;

import java.util.Date;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import casser.core.Casser;
import casser.core.CasserSession;
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
		
		for (int i = 0; i != 100; ++i) {
		
			post.setUserId(userId);
			post.setTimestamp(new Date(postTime+1000L*i));
			post.setText("hello");
			
			session.upsert(post).sync();
		}
		
		session.select(timeline::getUserId, timeline::getTimestamp, timeline::getText)
		.where(timeline::getUserId, "==", userId).sync()
		.forEach(t -> System.out.println(t));
		
	}
	
}
