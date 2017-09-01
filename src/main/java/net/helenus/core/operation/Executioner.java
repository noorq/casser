package net.helenus.core.operation;

import brave.Span;
import brave.Tracer;
import brave.propagation.TraceContext;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import net.helenus.core.AbstractSessionOperations;
import net.helenus.core.UnitOfWork;


public enum Executioner {
  INSTANCE;

  <E> E sync(AbstractSessionOperations session, UnitOfWork uow, Function<E, ?> extractor,
             TraceContext traceContext, OperationsDelegate<E> delegate, boolean showValues) {
    return this.<E>execute(session, uow, traceContext, delegate, showValues);
  }

  public <E> CompletableFuture<E> async(AbstractSessionOperations session, UnitOfWork uow, TraceContext traceContext,
      OperationsDelegate<E> delegate, boolean showValues) {
    return CompletableFuture.<E>supplyAsync(() -> execute(session, uow, traceContext, delegate, showValues));
  }

  public <E> E execute(AbstractSessionOperations session, UnitOfWork uow, TraceContext traceContext,
                       OperationsDelegate<E> delegate, boolean showValues) {

    // Start recording in a Zipkin sub-span our execution time to perform this operation.
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

      // Determine if we are caching and if so where to put the results.
      AbstractCache<CacheKey, Object> cache = null;
      boolean prepareStatementForCaching = false;
      if (uow != null ) {
        prepareStatementForCaching = true;
        cache = uow.getCacheEnclosing(delegate.getCache());
      } else {
        cache = delegate.getCache();
        prepareStatementForCaching = cache != null;
      }

      // TODO: first, ask the delegate for the cacheKey
      // if this is a SELECT query:
      //  if not in cache build the statement, execute the future, cache the result, transform the result then cache the transformations
      // if INSERT/UPSERT/UPDATE
      // if DELETE
      // if COUNT
      CacheKey key = (cache == null) ? null : delegate.getCacheKey();
      E result = null;

      if (key != null && cache != null) {
        // Right now we only support Optional<Entity> fetch via complete primary key.
        Object value = cache.get(key);
        if (value != null) {
          result = (E) Optional.of(value);
          // if log statements: log cache hit for entity, primary key
          // metrics.cacheHit +1
        } else {
          // if log statements: log cache miss for entity, primary key
          // metrics.cacheMiss +1
        }
      }

      ResultSet resultSet = null;
      if (result == null) {
        Statement statement = delegate.options(delegate.buildStatement(prepareStatementForCaching));
        // if log statements... log it here.
        ResultSetFuture futureResultSet = session.executeAsync(statement, showValues);
        resultSet = futureResultSet.get();
      }
      result = delegate.transform(resultSet);

      if (cache != null) {
        updateCache.apply(result, cache);
      }

      return (E) result;

    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    } finally {
      if (span != null) {
        span.finish();
      }
    }
  }

}
