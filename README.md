# casser            ![build](https://travis-ci.org/noorq/casser.svg?branch=master)
Java 8 Cassandra Client

Current status: Active development

### Features

* Leverages Java 8 language capabilities to build CQL queries
* Simple function-style API
* Reactive

### Requirements

* Only JVM 8
* Datastax Driver
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
