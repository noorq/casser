# casser
Java 8 Cassandra Client

### Features

* Leverages Java 8 language capabilities to build CQL queries
* Simple function-style API

### Requirements

* Only JVM 8
* Datastax Driver
* Maven

### Example

```
		session.select(timeline::getUserId, timeline::getTimestamp, timeline::getText)
		  .where(timeline::getUserId, "==", userId)
		  .orderBy(timeline::getTimestamp, "desc").limit(5).sync()
		  .forEach(System.out::println);
```
