# Helenus
Fast and easy, functional style cutting edge Java 8 Cassandra client for C* 3.x


### Features

* Leverages Java 8 language capabilities to build CQL queries
* Simple function-style stream API
* Reactive asynchronous and synchronous API
* Provides Java mapping for Tables, Tuples, UDTs (User Defined Type), Collections, UDT Collections, Tuple Collections
* Uses lazy mapping in all cases where possible
* Supports Java 8 Futures and Guava ListenableFuture

### Requirements

* JVM 8
* Datastax Driver 3.x
* Cassandra 3.x
* Maven

### Maven

Latest release dependency:
```
<dependencies>
	<dependency>
		<groupId>net.helenus</groupId>
		<artifactId>helenus-core</artifactId>
		<version>1.1.0_2.11</version>
	</dependency>
</dependencies>
```


### Simple Example

Model definition:
```
@Table
public interface Timeline {

	@PartitionKey
	UUID userId();

	@ClusteringColumn
	@Types.Timeuuid
	Date timestamp();

	@Column
	String text();

}
```

Session initialization:
```
HelenusSession session = Helenus.init(getSession()).showCql().add(Timeline.class).autoCreateDrop().get();
Timeline timeline = Helenus.dsl(Timeline.class, session.getMetadata());
```

Select example:
```
session.select(timeline::userId, timeline::timestamp, timeline::text)
  .where(timeline::userId, eq(userId))
  .orderBy(Query.desc(timeline::timestamp)).limit(5).sync()
  .forEach(System.out::println);
```

Insert example:
```
TimelineImpl post = new TimelineImpl();
post.userId=userId;
post.timestamp=new Date(postTime+1000L*i);
post.text="hello";
session.upsert(post).sync();
```

### Model and Repository Example

Account model:
```
@Table
public interface Account {
	@PartitionKey
	String accountId();
	Date createdAt();
	String organization();
	String team();
	String timezone();
	Map<String, AccountUser> users();
}
```

AccountUser model:
```
@UDT
public interface AccountUser {
	String email();
	String firstName();
	String lastName();
}
```

Abstract repository:
```
public interface AbstractRepository {
	HelenusSession session();
}
```

Account repository:
```

public interface AccountRepository extends AbstractRepository {

	static final Account account = Helenus.dsl(Account.class);
	static final String DEFAULT_TIMEZONE = "America/Los_Angeles";
	default Future<Optional<Account>> findAccount(String accountId) {

		return session()
				.select(Account.class)
				.where(account::accountId, eq(accountId))
				.single()
				.future();
	}

	default Future<Fun.Tuple2<ResultSet, String>> createAccount(
			String email,
			AccountUser user,
			String organization,
			String team,
			String timezone) {

		String accountId = AccountId.next();

		if (timezone == null || timezone.isEmpty()) {
			timezone = DEFAULT_TIMEZONE;
		}

		return session()
			.insert()
			.value(account::accountId, accountId)
			.value(account::createdAt, new Date())
			.value(account::organization, organization)
			.value(account::team, team)
			.value(account::timezone, timezone)
			.value(account::users, ImmutableMap.of(email, user))
			.future(accountId);

	}

	default Future<ResultSet> putAccountUser(String accountId, String email, AccountUser user) {

		return session()
				.update()
				.put(account::users, email.toLowerCase(), user)
				.where(account::accountId, eq(accountId))
				.future();

	}

	default Future<ResultSet> removeAccountUser(String accountId, String email) {

		return session()
				.update()
				.put(account::users, email.toLowerCase(), null)
				.where(account::accountId, eq(accountId))
				.future();

	}

	default Future<ResultSet> dropAccount(String accountId) {

		return session()
				.delete()
				.where(account::accountId, eq(accountId))
				.future();

	}

}
```
