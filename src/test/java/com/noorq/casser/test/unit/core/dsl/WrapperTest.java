/*
 *      Copyright (C) 2015 The Casser Authors
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
package com.noorq.casser.test.unit.core.dsl;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.noorq.casser.core.Casser;
import com.noorq.casser.support.CasserException;

public class WrapperTest {
	
	@Test
	public void testWrap() throws Exception {
		
		Map<String, Object> map = new HashMap<String, Object>();
	
		map.put("id", 123L);
		map.put("active", Boolean.TRUE);
		map.put("unknownField", "he-he");
		
		Account account = Casser.map(Account.class, map);
		
		Assert.assertEquals(Long.valueOf(123L), account.id());
		Assert.assertTrue(account.active());
		
	}
	
	@Test
	public void testPrimitive() throws Exception {
		
		Map<String, Object> map = new HashMap<String, Object>();
		
		map.put("id", 123L);
		
		Account account = Casser.map(Account.class, map);
		
		Assert.assertFalse(account.active());
				
	}
	
	@Test(expected=CasserException.class)
	public void testWrongMethods() throws Exception {
		
		WrongAccount wrongAccount = Casser.map(WrongAccount.class, new HashMap<String, Object>());
		
		wrongAccount.id();

	}
	
}
