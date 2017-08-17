package net.helenus.core.operation;

import brave.Span;
import brave.Tracer;
import brave.propagation.TraceContext;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import net.helenus.core.AbstractSessionOperations;
import net.helenus.support.HelenusException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public enum Executioner {
  INSTANCE;

    <E> E sync(
            AbstractSessionOperations session,
            Statement statement,
            TraceContext traceContext,
            OperationsDelegate<E> delegate,
            boolean showValues) {
        return sync(session, statement, null, traceContext, delegate, showValues);
    }

    <E> E sync(
            AbstractSessionOperations session,
            Statement statement,
            CacheManager cacheManager,
            TraceContext traceContext,
            OperationsDelegate<E> delegate,
            boolean showValues) {
    try {
      return this.<E>async(session, statement, cacheManager, traceContext, delegate, showValues).get();
    } catch (InterruptedException | ExecutionException e) {
        throw new HelenusException(e);
    }
  }

  public <E> CompletableFuture<E> async(
            AbstractSessionOperations session,
            Statement statement,
            TraceContext traceContext,
            OperationsDelegate<E> delegate,
            boolean showValues) {
        return async(session, statement, null, traceContext, delegate, showValues);
  }

  public <E> CompletableFuture<E> async(
            AbstractSessionOperations session,
            Statement statement,
            CacheManager cacheManager,
            TraceContext traceContext,
            OperationsDelegate<E> delegate,
            boolean showValues) {
    ResultSetFuture futureResultSet = session.executeAsync(statement, showValues);

    return CompletableFuture.<E>supplyAsync(
        () -> {
          Tracer tracer = session.getZipkinTracer();
          final Span span =
              (tracer != null && traceContext != null) ? tracer.newChild(traceContext) : null;
          try {
            if (span != null) {
              span.name("cassandra");
              span.start();
            }
            ResultSet resultSet = cacheManager != null ? cacheManager.apply(statement, delegate, futureResultSet) :
                    futureResultSet.get();
            E result = delegate.transform(resultSet);

            return result;
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          } finally {
            if (span != null) {
              span.finish();
            }
          }
        });
  }
}
