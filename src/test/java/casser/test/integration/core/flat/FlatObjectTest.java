package casser.test.integration.core.flat;


import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import casser.core.Casser;
import casser.core.CasserSession;
import casser.test.integration.build.AbstractEmbeddedCassandraTest;

public class FlatObjectTest extends AbstractEmbeddedCassandraTest {

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
	
		Long id = session.select(user::getId).where(user::getId, "==", 123L).sync().findFirst().get().v1;
		
		Assert.assertEquals(Long.valueOf(123L), id);
		
		session.delete().where(user::getId, "==", 123L).sync();
		
	}
	

	
}
