package net.helenus.core.operation;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import com.google.common.cache.Cache;
import java.util.concurrent.ExecutionException;

public abstract class AbstractCache {
  protected CacheManager.Type type;
  protected Cache<String, ResultSet> cache;

  public AbstractCache(CacheManager.Type type, Cache<String, ResultSet> cache) {
    this.type = type;
    this.cache = cache;
  }

  protected abstract ResultSet fetch(
      Statement statement, OperationsDelegate delegate, ResultSetFuture resultSetFuture)
      throws InterruptedException, ExecutionException;

  protected abstract ResultSet mutate(
      Statement statement, OperationsDelegate delegate, ResultSetFuture resultSetFuture)
      throws InterruptedException, ExecutionException;

  public ResultSet apply(
      Statement statement, OperationsDelegate delegate, ResultSetFuture futureResultSet)
      throws InterruptedException, ExecutionException {
    ResultSet resultSet = null;
    switch (type) {
      case FETCH:
        resultSet = fetch(statement, delegate, futureResultSet);
        break;
      case MUTATE:
        resultSet = mutate(statement, delegate, futureResultSet);
        break;
    }
    return resultSet;
  }
}
