package casser.core;

import org.junit.Test;

import casser.support.DslColumnException;

public class DslTest {

	@Test
	public void test() throws Exception {
		
		Account account = Casser.dsl(Account.class);
		
		System.out.println("account = " + account);
		
		try {
			account.getId();
		}
		catch(DslColumnException e) {
			System.out.println(e.getColumnInformation());
		}
		
	}
	
}
