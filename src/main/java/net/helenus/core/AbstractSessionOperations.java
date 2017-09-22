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
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.PrintStream;
import java.util.concurrent.Executor;

import net.helenus.mapping.value.ColumnValuePreparer;
import net.helenus.mapping.value.ColumnValueProvider;
import net.helenus.support.HelenusException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractSessionOperations {

  final Logger logger = LoggerFactory.getLogger(getClass());

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
      log(statement, false);
      return currentSession().prepare(statement);
    } catch (RuntimeException e) {
      throw translateException(e);
    }
  }

  public ListenableFuture<PreparedStatement> prepareAsync(RegularStatement statement) {
    try {
      log(statement, false);
      return currentSession().prepareAsync(statement);
    } catch (RuntimeException e) {
      throw translateException(e);
    }
  }

  public ResultSet execute(Statement statement, boolean showValues) {
    return executeAsync(statement, showValues).getUninterruptibly();
  }

  public ResultSetFuture executeAsync(Statement statement, boolean showValues) {
    try {
      log(statement, showValues);
      return currentSession().executeAsync(statement);
    } catch (RuntimeException e) {
      throw translateException(e);
    }
  }

  void log(Statement statement, boolean showValues) {
    if (logger.isInfoEnabled()) {
      logger.info("Execute statement " + statement);
    }
    if (isShowCql()) {
      if (statement instanceof BuiltStatement) {
        BuiltStatement builtStatement = (BuiltStatement) statement;
        if (showValues) {
          RegularStatement regularStatement = builtStatement.setForceNoValues(true);
          printCql(regularStatement.getQueryString());
        } else {
          printCql(builtStatement.getQueryString());
        }
      } else if (statement instanceof RegularStatement) {
        RegularStatement regularStatement = (RegularStatement) statement;
        printCql(regularStatement.getQueryString());
      } else {
        printCql(statement.toString());
      }
    }
  }

  public Tracer getZipkinTracer() {
    return null;
  }

  public MetricRegistry getMetricRegistry() {
    return null;
  }

  RuntimeException translateException(RuntimeException e) {
    if (e instanceof HelenusException) {
      return e;
    }
    throw new HelenusException(e);
  }

  void printCql(String cql) {
    getPrintStream().println(cql);
  }

}
