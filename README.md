# helenus
Fast and easy, functional style cutting edge Java 8 and Scala 2.11 Cassandra client

Current status: First application in production (may be more)

### Features

* Leverages Java 8 language capabilities to build CQL queries
* Simple function-style stream API
* Reactive asynchronous and synchronous API
* Provides Java mapping for Tables, Tuples, UDTs (User Defined Type), Collections, UDT Collections, Tuple Collections
* Uses lazy mapping in all cases where possible
* Supports Guava ListenableFuture and Scala Future

### Requirements

* Latest JVM 8
* Latest Datastax Driver 2.1.5
* Latest Cassandra 2.1.4
* Latest Scala 2.11
* Latest Maven as well

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

Active development dependency for Scala 2.11:
```
<dependencies>
	<dependency>
		<groupId>net.helenus</groupId>
		<artifactId>helenus-core</artifactId>
		<version>1.2.0_2.11-SNAPSHOT</version>
	</dependency>
</dependencies>

<repositories>
    <repository>
        <id>oss-sonatype</id>
        <name>oss-sonatype</name>
        <url>https://oss.sonatype.org/content/repositories/snapshots/</url>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
    </repository>
</repositories>
```

### Simple Example

Model definition:
```
@Table("timelines")
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
Timeline timeline = Helenus.dsl(Timeline.class);
HelenusSession session = Helenus.init(getSession()).showCql().add(Timeline.class).autoCreateDrop().get();
```

Select example:
```
session.select(timeline::userId, timeline::timestamp, timeline::text)
  .where(timeline::userId, Query.eq(userId))
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
import scala.concurrent.Future;

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
