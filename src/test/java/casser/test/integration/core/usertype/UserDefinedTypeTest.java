package casser.test.integration.core.usertype;

import org.junit.Before;
import org.junit.Test;

import casser.core.Casser;
import casser.core.CasserSession;
import casser.test.integration.build.AbstractEmbeddedCassandraTest;

public class UserDefinedTypeTest extends AbstractEmbeddedCassandraTest {

	Account account;
	
	CasserSession session;
	
	@Before
	public void beforeTest() {
		
		account = Casser.dsl(Account.class);
		
		//session = Casser.init(getSession()).showCql().createDrop(Account.class).get();
	}
	
	@Test
	public void testUDT() {
		
		System.out.println("test it");
		
	}
}
