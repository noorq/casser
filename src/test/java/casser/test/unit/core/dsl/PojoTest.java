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
