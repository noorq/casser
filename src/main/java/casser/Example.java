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
package casser;

import com.datastax.driver.core.Cluster;

import casser.core.Casser;
import casser.core.Filter;
import casser.core.Prepared;
import casser.core.CasserSession;
import casser.core.operation.SelectOperation;
import casser.core.tuple.Tuple1;
import casser.core.tuple.Tuple2;

public class Example {

	static final User _user = Casser.dsl(User.class);
	
	Cluster cluster = new Cluster.Builder().addContactPoint("localhost").build();
	
	CasserSession session = Casser.connect(cluster).use("test").update(_user).get();
	
	public static User mapUser(Tuple2<String, Integer> t) {
		User user = Casser.pojo(User.class);
		user.setName(t.v1);
		user.setAge(t.v2);
		return user;
	}
	
	public void test() {
		
		User newUser = Casser.pojo(User.class);
		newUser.setId(100L);
		newUser.setName("alex");
		newUser.setAge(34);
		session.upsert(newUser);
		
		String nameAndAge = session.select(_user::getName, _user::getAge).where(_user::getId, "==", 100L).sync().findFirst().map(t -> {
			return t.v1 + ":" +  t.v2;
		}).get();

		User user = session.select(_user::getName, _user::getAge).where(_user::getId, "==", 100L).map(Example::mapUser).sync().findFirst().get();

		session.update(_user::setAge, 10).where(_user::getId, "==", 100L).async();
		
		session.delete(User.class).where(_user::getId, "==", 100L).async();
		
		Prepared<SelectOperation<Tuple1<String>>> ps = session.select(_user::getName).where(_user::getId, "==", null).prepare();
		
		long cnt = ps.bind(100L).sync().count();
		
		cnt = session.select(_user::getName).where(_user::getId, "==", 100L).count().sync();

		cnt = session.select(_user::getName).where(Filter.equal(_user::getId, 100L)).count().sync();

	}
	
}
