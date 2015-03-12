package casser;

import casser.core.Casser;
import casser.core.Session;
import casser.tuple.Tuple2;

public class Example {

	User userDsl = Casser.dsl(User.class);
	
	Session session = Casser.connect("localhost").update(userDsl).get();
	
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
		
		String nameAndAge = session.select(userDsl::getName, userDsl::getAge).where(userDsl::getId,  100L).sync().findFirst().map(t -> {
			return t.v1 + ":" +  t.v2;
		}).get();

		User user = session.select(userDsl::getName, userDsl::getAge).where(userDsl::getId,  100L).map(Example::mapUser).sync().findFirst().get();

		session.update(userDsl::setAge, 10).where(userDsl::getId, 100L).async();
		
		session.delete().where(userDsl::getId, 100L).async();
		
	}
	
}
