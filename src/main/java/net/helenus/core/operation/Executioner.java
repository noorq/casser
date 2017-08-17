package net.helenus.core.operation;

import brave.Span;
import brave.Tracer;
import brave.propagation.TraceContext;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import net.helenus.core.AbstractSessionOperations;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public enum Executioner {
  INSTANCE;

  <E> E sync(
      AbstractSessionOperations session,
      Statement statement,
      TraceContext traceContext,
      Transformational<E> delegate,
      boolean showValues) {
    try {
      return this.<E>async(session, statement, traceContext, delegate, showValues).get();
    } catch (InterruptedException | ExecutionException e) {
      return null;
    }
  }

  public <E> CompletableFuture<E> async(
      AbstractSessionOperations session,
      Statement statement,
      TraceContext traceContext,
      Transformational<E> delegate,
      boolean showValues) {
    ResultSetFuture futureResultSet = session.executeAsync(statement, showValues);

    return CompletableFuture.supplyAsync(
        () -> {
          Tracer tracer = session.getZipkinTracer();
          final Span span =
              (tracer != null && traceContext != null) ? tracer.newChild(traceContext) : null;
          try {
            if (span != null) {
              span.name("cassandra");
              span.start();
            }
            ResultSet resultSet = futureResultSet.get(); // TODO: timeout
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
