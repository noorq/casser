package net.helenus.core.operation;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;


interface OperationsDelegate<E> {

  Statement buildStatement(boolean cached);

  Statement options(Statement statement);

  E transform(ResultSet resultSet);

  AbstractCache getCache();

  CacheKey getCacheKey();
}
