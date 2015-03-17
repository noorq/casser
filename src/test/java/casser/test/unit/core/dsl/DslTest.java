package casser.test.unit.core.dsl;

import org.junit.Test;

import casser.core.Casser;
import casser.support.DslPropertyException;

public class DslTest {

	@Test
	public void test() throws Exception {
		
		Account account = Casser.dsl(Account.class);
		
		System.out.println("account = " + account);
		
		try {
			account.getId();
		}
		catch(DslPropertyException e) {
			System.out.println(e.getProperty());
		}
		
	}
	
}
