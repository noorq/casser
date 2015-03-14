package casser;

import casser.core.Casser;
import casser.core.PreparedStreamStatement;
import casser.core.Session;
import casser.operation.SelectOperation;
import casser.tuple.Tuple1;
import casser.tuple.Tuple2;

public class Example {

	static final User _user = Casser.dsl(User.class);
	
	Session session = Casser.connect("localhost").update(_user).get();
	
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
		
		session.delete().where(_user::getId, "==", 100L).async();
		
		
		PreparedStreamStatement<Tuple1<String>, SelectOperation<Tuple1<String>>> ps = session.select(_user::getName).where(_user::getId, "==", null).prepare();
		
		long cnt = ps.bind(100L).sync().count();
		
		cnt = session.select(_user::getName).where(_user::getId, "==", 100L).count().sync();
		
	}
	
}
