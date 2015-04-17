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



import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.noorq.casser.core.Casser;
import com.noorq.casser.core.Getter;
import com.noorq.casser.core.Query;
import com.noorq.casser.core.reflect.CasserPropertyNode;
import com.noorq.casser.support.DslPropertyException;

public class CollectionsDlsTest {

	static AccountWithCollections account;
	
	@BeforeClass
	public static void beforeTests() {
		account = Casser.dsl(AccountWithCollections.class);
	}
	
	@Test
	public void testPrint() {
		System.out.println(account);
	}
	
	@Test
	public void testMapGet() {
		
		String columnName = null;
		
		Getter<String> getter = Query.get(account::properties, "key1");
		
		try {
			getter.get();
		}
		catch(DslPropertyException e) {
			
			CasserPropertyNode node = e.getPropertyNode();
			columnName = node.getColumnName();
			
		}
		
		Assert.assertEquals("\"properties\"[\"key1\"]", columnName);
		
	}
	
	@Test
	public void testListGet() {
		
		String columnName = null;
		
		Getter<String> getter = Query.getIdx(account::name, 2);
		
		try {
			getter.get();
		}
		catch(DslPropertyException e) {
			
			CasserPropertyNode node = e.getPropertyNode();

			columnName = node.getColumnName();
			
		}
		
		Assert.assertEquals("\"name\"[\"2\"]", columnName);
		
	}
	
}
