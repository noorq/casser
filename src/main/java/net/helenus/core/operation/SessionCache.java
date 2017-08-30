package net.helenus.core.operation;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import com.google.common.cache.Cache;
import java.util.concurrent.ExecutionException;

public class SessionCache extends AbstractCache<String, ResultSet> {

  protected ResultSet apply(
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
          cache.put(cacheKey, resultSet);
        }
      }
    }
    return resultSet;
  }

  public ResultSet get(Statement statement, OperationsDelegate delegate) {
    final CacheKey key = delegate.getCacheKey();
    final String cacheKey = (key == null) ? CacheKey.of(statement) : key.toString();
    return cache.getIfPresent(cacheKey);
  }
}
