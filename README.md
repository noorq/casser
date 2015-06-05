# casser
Fast and easy, functional style cutting edge Java 8 and Scala 2.11 Cassandra client

Current status: First application in production (may be more)

### Features

* Leverages Java 8 language capabilities to build CQL queries
* Simple function-style stream API
* Reactive asynchronous and synchronous API
* Provides Java mapping for Tables, Tuples, UDTs (User Defined Type), Collections, UDT Collections, Tuple Collections
* Provides Lazy mapping in all cases where possible

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
		<groupId>com.noorq.casser</groupId>
		<artifactId>casser-core</artifactId>
		<version>1.0.0</version>
	</dependency>
</dependencies>
```

Active development dependency:
```
<dependencies>
	<dependency>
		<groupId>com.noorq.casser</groupId>
		<artifactId>casser-core</artifactId>
		<version>1.1.0-SNAPSHOT</version>
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

### Example

Entity definition:
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
Timeline timeline = Casser.dsl(Timeline.class);
CasserSession session = Casser.init(getSession()).showCql().add(Timeline.class).autoCreateDrop().get();
```

Select information:
```
session.select(timeline::userId, timeline::timestamp, timeline::text)
  .where(timeline::userId, Query.eq(userId))
  .orderBy(Query.desc(timeline::timestamp)).limit(5).sync()
  .forEach(System.out::println);
```

Insert information:
```
TimelineImpl post = new TimelineImpl();
post.userId=userId;
post.timestamp=new Date(postTime+1000L*i);
post.text="hello";
session.upsert(post).sync();
```
