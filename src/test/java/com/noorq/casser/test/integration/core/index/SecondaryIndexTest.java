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
