package net.helenus.core.operation;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import com.google.common.cache.Cache;
import java.util.concurrent.ExecutionException;

public class SessionCache extends AbstractCache<CacheKey, ResultSet> {

  protected ResultSet apply(CacheKey key, OperationsDelegate delegate)
      throws InterruptedException, ExecutionException {


    ResultSet resultSet = null;
    resultSet = cache.getIfPresent(key);

    if (resultSet != null) {
      cache.put(key, resultSet);
    }

    return resultSet;
  }

}
