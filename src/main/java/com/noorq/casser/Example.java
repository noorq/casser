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
package com.noorq.casser;

import static com.noorq.casser.core.Query.eq;

import com.datastax.driver.core.Cluster;
import com.noorq.casser.core.Casser;
import com.noorq.casser.core.CasserSession;
import com.noorq.casser.core.operation.PreparedStreamOperation;
import com.noorq.casser.core.tuple.Tuple1;
import com.noorq.casser.core.tuple.Tuple2;

public class Example {

	static final User user = Casser.dsl(User.class);
	
	Cluster cluster = new Cluster.Builder().addContactPoint("localhost").build();
	
	CasserSession session = Casser.connect(cluster).use("test").add(user).autoUpdate().get();
	
	public static User mapUser(final Tuple2<String, Integer> t) {
		
		return new User() {

			@Override
			public Long id() {
				return null;
			}

			@Override
			public String name() {
				return t._1;
			}

			@Override
			public Integer age() {
				return t._2;
			}
			
		};
		
	}
	
	public static class UserImpl implements User {
		
		Long id;
		String name;
		Integer age;
		
		@Override
		public Long id() {
			return id;
		}
		
		@Override
		public String name() {
			return name;
		}
		
		@Override
		public Integer age() {
			return age;
		}
		
	}
	
	public void test() {
		
		UserImpl newUser = new UserImpl();
		newUser.id = 100L;
		newUser.name = "alex";
		newUser.age = 34;
		session.upsert(newUser);
		
		String nameAndAge = session.select(user::name, user::age).where(user::id, eq(100L)).sync().findFirst().map(t -> {
			return t._1 + ":" +  t._2;
		}).get();

		User userTmp = session.select(user::name, user::age).where(user::id, eq(100L)).map(Example::mapUser).sync().findFirst().get();

		session.update(user::age, 10).where(user::id, eq(100L)).async();
		
		session.delete(User.class).where(user::id, eq(100L)).async();
		
		PreparedStreamOperation<Tuple1<String>> ps = session.select(user::name).where(user::id, "==", null).prepare();
		
		long cnt = ps.bind(100L).sync().count();
		
		cnt = session.select(user::name).where(user::id, eq(100L)).count().sync();

		cnt = session.select(user::name).where(user::id, eq(100L)).count().sync();

	}
	
}
