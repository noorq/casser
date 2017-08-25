package net.helenus.core.operation;

import com.datastax.driver.core.ResultSet;

public interface OperationsDelegate<E> {
  E transform(ResultSet resultSet);

  CacheKey getCacheKey();
}
