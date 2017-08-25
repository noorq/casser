package net.helenus.core.operation;

import brave.Span;
import brave.Tracer;
import brave.propagation.TraceContext;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import net.helenus.core.AbstractSessionOperations;
import net.helenus.core.UnitOfWork;

public enum Executioner {
  INSTANCE;

  <E> E sync(
      AbstractSessionOperations session,
      UnitOfWork uow,
      Statement statement,
      AbstractCache cache,
      TraceContext traceContext,
      OperationsDelegate<E> delegate,
      boolean showValues) {
    ResultSetFuture futureResultSet = session.executeAsync(statement, showValues);
    return this.<E>execute(
        futureResultSet, session, uow, statement, cache, traceContext, delegate, showValues);
  }

  public <E> CompletableFuture<E> async(
      AbstractSessionOperations session,
      UnitOfWork uow,
      Statement statement,
      AbstractCache cache,
      TraceContext traceContext,
      OperationsDelegate<E> delegate,
      boolean showValues) {

    ResultSetFuture futureResultSet = session.executeAsync(statement, showValues);
    return CompletableFuture.<E>supplyAsync(
        () ->
            execute(
                futureResultSet, session, uow, statement, cache, traceContext, delegate, showValues));
  }

  public <E> E execute(
      ResultSetFuture futureResultSet,
      AbstractSessionOperations session,
      UnitOfWork uow,
      Statement statement,
      AbstractCache cache,
      TraceContext traceContext,
      OperationsDelegate<E> delegate,
      boolean showValues) {

    Tracer tracer = session.getZipkinTracer();
    Span span = null;
    if (tracer != null && traceContext != null) {
      span = tracer.newChild(traceContext);
    }

    try {
      if (span != null) {
        span.name("cassandra");
        span.start();
      }

      ResultSet resultSet;
      if (cache != null) {
        resultSet = cache.apply(statement, delegate, futureResultSet);
      } else {
        resultSet = futureResultSet.get();
      }

      E result = delegate.transform(resultSet);

      return result;
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    } finally {
      if (span != null) {
        span.finish();
      }
    }
  }
}
