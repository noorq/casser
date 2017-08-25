package net.helenus.core.operation;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import com.google.common.cache.Cache;
import java.util.concurrent.ExecutionException;

public class SessionCache extends AbstractCache {

  private final CacheManager manager;

  SessionCache(CacheManager.Type type, CacheManager manager, Cache<String, ResultSet> cache) {
    super(type, cache);
    this.manager = manager;
  }

  protected ResultSet fetch(
      Statement statement, OperationsDelegate delegate, ResultSetFuture resultSetFuture)
      throws InterruptedException, ExecutionException {
    final CacheKey key = delegate.getCacheKey();
    final String cacheKey = (key == null) ? CacheKey.of(statement) : key.toString();
    ResultSet resultSet = null;
    if (cacheKey == null) {
      resultSet = resultSetFuture.get();
    } else {
      resultSet = cache.getIfPresent(cacheKey);
      if (resultSet == null) {
        resultSet = resultSetFuture.get();
        if (resultSet != null) {
          planEvictionFor(statement);
          cache.put(cacheKey, resultSet);
        }
      }
    }
    return resultSet;
  }

  protected ResultSet mutate(
      Statement statement, OperationsDelegate delegate, ResultSetFuture resultSetFuture)
      throws InterruptedException, ExecutionException {
    CacheKey key = delegate.getCacheKey();
    final String cacheKey = key == null ? statement.toString() : key.toString();
    ResultSet resultSet = resultSetFuture.get();
    if (cacheKey != null && resultSet != null) {
      planEvictionFor(statement);
      //manager.evictIfNecessary(statement, delegate);
      cache.put(cacheKey, resultSet);
    }
    return resultSet;
  }

  private void planEvictionFor(Statement statement) {
    //((Select)statement).table + statement.where.clauses.length == 0
    //TTL for rows read
  }

  public ResultSet get(Statement statement, OperationsDelegate delegate) {
    final CacheKey key = delegate.getCacheKey();
    final String cacheKey = (key == null) ? CacheKey.of(statement) : key.toString();
    return cache.getIfPresent(cacheKey);
  }
}
