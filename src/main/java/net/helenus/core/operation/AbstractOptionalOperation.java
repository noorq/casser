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

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import net.helenus.core.AbstractSessionOperations;
import net.helenus.core.UnitOfWork;

public abstract class AbstractOptionalOperation<E, O extends AbstractOptionalOperation<E, O>>
    extends AbstractStatementOperation<E, O> {

  public AbstractOptionalOperation(AbstractSessionOperations sessionOperations) {
    super(sessionOperations);
  }

  public abstract Optional<E> transform(ResultSet resultSet);

  public PreparedOptionalOperation<E> prepare() {
    return new PreparedOptionalOperation<E>(prepareStatement(), this);
  }

  public ListenableFuture<PreparedOptionalOperation<E>> prepareAsync() {
    final O _this = (O) this;
    return Futures.transform(
        prepareStatementAsync(),
        new Function<PreparedStatement, PreparedOptionalOperation<E>>() {
          @Override
          public PreparedOptionalOperation<E> apply(PreparedStatement preparedStatement) {
            return new PreparedOptionalOperation<E>(preparedStatement, _this);
          }
        });
  }

  public Optional<E> sync() {
    final Timer.Context context = requestLatency.time();
    try {
      ResultSet resultSet = this.execute(sessionOps, null, traceContext, showValues, false);
      return transform(resultSet);
    } finally {
      context.stop();
    }
  }

  public Optional<E> sync(UnitOfWork uow) {
    if (uow == null) return sync();

    final Timer.Context context = requestLatency.time();
    try {

      Optional<E> result = null;
      String key = getStatementCacheKey();
      if (enableCache && key != null) {
        Set<E> cachedResult = (Set<E>) uow.cacheLookup(key);
        if (cachedResult != null) {
          //TODO(gburd): what about select ResultSet, Tuple... etc.?
          uowCacheHits.mark();
          logger.info("UOW({}) cache hit, {}", uow.hashCode(), key);
          result = cachedResult.stream().findFirst();
        } else {
          uowCacheMiss.mark();
        }
      }

      if (result == null) {
        ResultSet resultSet = execute(sessionOps, uow, traceContext, showValues, true);
        result = transform(resultSet);

        if (key != null) {
          if (result.isPresent()) {
            Set<Object> set = new HashSet<Object>(1);
            set.add(result.get());
            uow.getCache().put(key, set);
          } else {
            uow.getCache().put(key, new HashSet<Object>(0));
          }
        }
      }

      return result;
    } finally {
      context.stop();
    }
  }

  public CompletableFuture<Optional<E>> async() {
    return CompletableFuture.<Optional<E>>supplyAsync(() -> sync());
  }

  public CompletableFuture<Optional<E>> async(UnitOfWork uow) {
    if (uow == null) return async();
    return CompletableFuture.<Optional<E>>supplyAsync(() -> sync(uow));
  }

}
