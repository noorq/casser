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

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.helenus.core.AbstractSessionOperations;
import net.helenus.core.Helenus;
import net.helenus.core.UnitOfWork;
import net.helenus.mapping.HelenusEntity;
import net.helenus.mapping.value.ColumnValueProvider;
import net.helenus.mapping.value.ValueProviderMap;

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

  public Stream<E> sync() {
    final Timer.Context context = requestLatency.time();
    try {
      ResultSet resultSet = this.execute(sessionOps, null, traceContext, showValues, false);
      return transform(resultSet);
    } finally {
      context.stop();
    }
  }

  public Stream<E> sync(UnitOfWork uow) {
    Objects.requireNonNull(uow, "Unit of Work should not be null.");

    final Timer.Context context = requestLatency.time();
    try {
      Stream<E> result = null;
      String key = getStatementCacheKey();
      if (key != null) {
        Set<E> cachedResult = (Set<E>) uow.cacheLookup(key);
        if (cachedResult != null) {
          //TODO(gburd): what about select ResultSet, Tuple... etc.?
          uowCacheHits.mark();
          logger.info("UOW({}) cache hit, {} -> {}", uow.hashCode(), key, cachedResult.toString());
          result = cachedResult.stream();
        }
      }

      if (result == null) {
        uowCacheMiss.mark();
        ResultSet resultSet = execute(sessionOps, uow, traceContext, showValues, true);
        result = transform(resultSet);

        if (key != null) {
          uow.getCache().put(key, (Set<Object>) result);
        }
      }

      return result;
    } finally {
      context.stop();
    }
  }

  public CompletableFuture<Stream<E>> async() {
    return CompletableFuture.<Stream<E>>supplyAsync(() -> sync());
  }

  public CompletableFuture<Stream<E>> async(UnitOfWork uow) {
    Objects.requireNonNull(uow, "Unit of Work should not be null.");

    return CompletableFuture.<Stream<E>>supplyAsync(() -> sync(uow));
  }

}
