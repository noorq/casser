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
package com.noorq.casser.test.integration.core.udtcollection;

import java.util.HashSet;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import com.noorq.casser.core.Casser;
import com.noorq.casser.core.CasserSession;
import com.noorq.casser.test.integration.build.AbstractEmbeddedCassandraTest;

public class UDTCollectionTest extends AbstractEmbeddedCassandraTest {

	static Book book = Casser.dsl(Book.class);
	
	static CasserSession session;

	@BeforeClass
	public static void beforeTest() {
		session = Casser.init(getSession()).showCql().add(book).autoCreateDrop().get();
	}
	
	@Test
	public void test() {
		System.out.println(book);	
	}
	
	@Test
	public void testSetCRUID() {
		
		int id = 555;
		
		// CREATE
		
		Set<Author> reviewers = new HashSet<Author>();
		reviewers.add(new AuthorImpl("Alex", "San Jose"));
		reviewers.add(new AuthorImpl("Bob", "San Francisco"));
		
		session.insert()
		.value(book::id, id)
		.value(book::reviewers, reviewers)
		.sync();
		
		
		
	}
	
	
	private static final class AuthorImpl implements Author {

		String name;
		String city;
		
		AuthorImpl(String name, String city) {
			this.name = name;
			this.city = city;
		}
		
		@Override
		public String name() {
			return name;
		}

		@Override
		public String city() {
			return city;
		}
		
	}

	private static final class SectionImpl implements Section {

		String title;
		int page;
		
		SectionImpl(String title, int page) {
			this.title = title;
			this.page = page;
		}
		
		@Override
		public String title() {
			return title;
		}

		@Override
		public int page() {
			return page;
		}
		
	}
	
	
}


