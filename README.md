# casser
Java 8 Cassandra Client ![build](https://travis-ci.org/noorq/casser.svg)

### Features

* Leverages Java 8 language capabilities to build CQL queries
* Simple function-style API

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
	UUID getUserId();
	
	void setUserId(UUID uid);
	
	@ClusteringColumn
	@DataTypeName(Name.TIMEUUID)
	Date getTimestamp();
	
	void setTimestamp(Date ts);
	
	@Column
	String getText();
	
	void setText(String text);
}
```

Session initialization:
```
Timeline timeline = Casser.dsl(Timeline.class);
CasserSession session = Casser.init(getSession()).showCql().add(Timeline.class).autoCreateDrop().get();
```

Select information:
```
session.select(timeline::getUserId, timeline::getTimestamp, timeline::getText)
  .where(timeline::getUserId, "==", userId)
  .orderBy(timeline::getTimestamp, "desc").limit(5).sync()
  .forEach(System.out::println);
```

Insert information:
```
Timeline post = Casser.pojo(Timeline.class);
post.setUserId(userId);
post.setTimestamp(new Date(postTime+1000L*i));
post.setText("hello");
session.upsert(post).sync();
```
