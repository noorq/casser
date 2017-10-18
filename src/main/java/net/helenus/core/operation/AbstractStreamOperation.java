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
package net.helenus.core.operation;

import com.codahale.metrics.Timer;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import net.helenus.core.AbstractSessionOperations;
import net.helenus.core.UnitOfWork;
import net.helenus.core.cache.EntityIdentifyingFacet;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

public abstract class AbstractStreamOperation<E, O extends AbstractStreamOperation<E, O>>
    extends AbstractStatementOperation<E, O> {

  public AbstractStreamOperation(AbstractSessionOperations sessionOperations) {
    super(sessionOperations);
  }

  public abstract Stream<E> transform(ResultSet resultSet);

  public PreparedStreamOperation<E> prepare() {
    return new PreparedStreamOperation<E>(prepareStatement(), this);
  }

  public ListenableFuture<PreparedStreamOperation<E>> prepareAsync() {
    final O _this = (O) this;
    return Futures.transform(
        prepareStatementAsync(),
        new Function<PreparedStatement, PreparedStreamOperation<E>>() {
          @Override
          public PreparedStreamOperation<E> apply(PreparedStatement preparedStatement) {
            return new PreparedStreamOperation<E>(preparedStatement, _this);
          }
        });
  }

  public Stream<E> sync() throws TimeoutException {
    final Timer.Context context = requestLatency.time();
    try {
      ResultSet resultSet = this.execute(sessionOps, null, traceContext, queryExecutionTimeout, queryTimeoutUnits, showValues, false);
      return transform(resultSet);
    } finally {
      context.stop();
    }
  }

  public Stream<E> sync(UnitOfWork<?> uow) throws TimeoutException {
    if (uow == null)
        return sync();

    final Timer.Context context = requestLatency.time();
    try {
        Stream<E> result = null;
        E cachedResult = null;
        String[] statementKeys = null;

        if (enableCache) {
            Set<EntityIdentifyingFacet> facets = getFacets();
            statementKeys = getQueryKeys();
            cachedResult = checkCache(uow, facets, statementKeys);
            if (cachedResult != null) {
                result = Stream.of(cachedResult);
            }
        }

        if (result == null) {
            ResultSet resultSet = execute(sessionOps, uow, traceContext, queryExecutionTimeout, queryTimeoutUnits,
                    showValues, true);
            result = transform(resultSet);
        }

        // If we have a result and we're caching then we need to put it into the cache for future requests to find.
        if (enableCache && cachedResult != null) {
            updateCache(uow, cachedResult, statementKeys);
        }

      return result;
    } finally {
      context.stop();
    }
  }

  public CompletableFuture<Stream<E>> async() {
    return CompletableFuture.<Stream<E>>supplyAsync(() -> {
        try {
            return sync();
        } catch (TimeoutException ex) { throw new CompletionException(ex); }
    });
  }

  public CompletableFuture<Stream<E>> async(UnitOfWork<?> uow) {
    if (uow == null) return async();
    return CompletableFuture.<Stream<E>>supplyAsync(() -> {
        try {
            return sync();
        } catch (TimeoutException ex) { throw new CompletionException(ex); }
    });
  }
}
