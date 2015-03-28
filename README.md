# casser            ![build](https://travis-ci.org/noorq/casser.svg?branch=master)
Cutting edge Java 8 Cassandra Client

Current status: Active development

### Features

* Leverages Java 8 language capabilities to build CQL queries
* Simple function-style stream API
* Reactive asynchronous and synchronous API
* Provides Java mapping for Tables and User Defined Types

### Requirements

* Only JVM 8
* Latest Datastax Driver 2.1.5
* Latest Cassandra
* Maven

### Example

Entity definition:
```
@Table("timelines")
public interface Timeline {

	@PartitionKey
	UUID userId();
	
	@ClusteringColumn
	@DataTypeName(Name.TIMEUUID)
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
  .where(timeline::userId, Operator.EQ, userId)
  .orderBy(timeline::timestamp, "desc").limit(5).sync()
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
