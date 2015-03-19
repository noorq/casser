package casser.test.integration.core;

import org.junit.Test;

import casser.core.Casser;
import casser.core.CasserSession;
import casser.test.integration.build.AbstractEmbeddedCassandraTest;

public class ContextInitTest extends AbstractEmbeddedCassandraTest {

	@Test
	public void test() {

		CasserSession session = Casser.init(getSession()).get();

		System.out.println("Works! " + session);
		
	}
	
}
