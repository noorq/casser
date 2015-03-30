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
package com.noorq.casser.test.unit.core.dsl;

import org.junit.Test;

import com.noorq.casser.core.Casser;
import com.noorq.casser.support.DslPropertyException;

public class DslTest {

	@Test
	public void test() throws Exception {
		
		Account account = Casser.dsl(Account.class);
		
		System.out.println("account = " + account);
		
		try {
			account.id();
		}
		catch(DslPropertyException e) {
			System.out.println(e.getPropertyNode().getProperty());
		}
		
	}
	
}
