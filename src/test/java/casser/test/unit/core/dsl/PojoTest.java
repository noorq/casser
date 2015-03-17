package casser.test.unit.core.dsl;

import org.junit.Assert;
import org.junit.Test;

import casser.core.Casser;
import casser.support.CasserException;

public class PojoTest {

	Account account = Casser.pojo(Account.class);
	
	@Test
	public void testObject() throws Exception {
		
		Assert.assertNull(account.getId());
		
		account.setId("testAcc");

		Assert.assertEquals("testAcc", account.getId());

		
	}
	
	@Test
	public void testPrimitive() throws Exception {
		
		Assert.assertFalse(account.isActive());
		
		account.setActive(true);

		Assert.assertEquals(true, account.isActive());

		
	}
	
	@Test(expected=CasserException.class)
	public void testWrongMethods() throws Exception {
		
		Casser.pojo(WrongAccount.class);

	}
	
}
