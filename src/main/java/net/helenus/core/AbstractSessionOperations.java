/*
 *      Copyright (C) 2015 The Helenus Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package net.helenus.core;

import brave.Tracer;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.*;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.Executor;
import net.helenus.core.cache.Facet;
import net.helenus.mapping.value.ColumnValuePreparer;
import net.helenus.mapping.value.ColumnValueProvider;
import net.helenus.support.Either;
import net.helenus.support.HelenusException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractSessionOperations {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSessionOperations.class);

  public abstract Session currentSession();

  public abstract String usingKeyspace();

  public abstract boolean isShowCql();

  public abstract PrintStream getPrintStream();

  public abstract Executor getExecutor();

  public abstract SessionRepository getSessionRepository();

  public abstract ColumnValueProvider getValueProvider();

  public abstract ColumnValuePreparer getValuePreparer();

  public abstract ConsistencyLevel getDefaultConsistencyLevel();

  public abstract boolean getDefaultQueryIdempotency();

  public PreparedStatement prepare(RegularStatement statement) {
    try {
      return currentSession().prepare(statement);
    } catch (RuntimeException e) {
      throw translateException(e);
    }
  }

  public ListenableFuture<PreparedStatement> prepareAsync(RegularStatement statement) {
    try {
      return currentSession().prepareAsync(statement);
    } catch (RuntimeException e) {
      throw translateException(e);
    }
  }

  public ResultSet execute(Statement statement) {
    return execute(statement, null, null);
  }

  public ResultSet execute(Statement statement, Stopwatch timer) {
    return execute(statement, null, timer);
  }

  public ResultSet execute(Statement statement, UnitOfWork uow) {
    return execute(statement, uow, null);
  }

  public ResultSet execute(Statement statement, UnitOfWork uow, Stopwatch timer) {
    return executeAsync(statement, uow, timer).getUninterruptibly();
  }

  public ResultSetFuture executeAsync(Statement statement) {
    return executeAsync(statement, null, null);
  }

  public ResultSetFuture executeAsync(Statement statement, Stopwatch timer) {
    return executeAsync(statement, null, timer);
  }

  public ResultSetFuture executeAsync(Statement statement, UnitOfWork uow) {
    return executeAsync(statement, uow, null);
  }

  public ResultSetFuture executeAsync(Statement statement, UnitOfWork uow, Stopwatch timer) {
    try {
      return currentSession().executeAsync(statement);
    } catch (RuntimeException e) {
      throw translateException(e);
    }
  }

  public Tracer getZipkinTracer() {
    return null;
  }

  public MetricRegistry getMetricRegistry() {
    return null;
  }

  public void mergeCache(Table<String, String, Either<Object, List<Facet>>> uowCache) {}

  RuntimeException translateException(RuntimeException e) {
    if (e instanceof HelenusException) {
      return e;
    }
    throw new HelenusException(e);
  }

  public Object checkCache(String tableName, List<Facet> facets) {
    return null;
  }

  public void updateCache(Object pojo, List<Facet> facets) {}

  public void cacheEvict(List<Facet> facets) {}
}
