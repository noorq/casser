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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import net.helenus.core.AbstractSessionOperations;
import net.helenus.core.Filter;
import net.helenus.core.Helenus;
import net.helenus.core.UnitOfWork;
import net.helenus.core.cache.EntityIdentifyingFacet;
import net.helenus.mapping.HelenusEntity;
import net.helenus.mapping.HelenusProperty;
import net.helenus.mapping.value.BeanColumnValueProvider;
import net.helenus.support.Either;

import javax.swing.text.html.parser.Entity;

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
      String stmtKey = null;
      if (enableCache) {
          Set<EntityIdentifyingFacet> facets = getIdentifyingFacets();
          if (!facets.isEmpty()) {
              for (EntityIdentifyingFacet facet : facets) {
                  //TODO(gburd): what about select ResultSet, Tuple... etc.?
                  Optional<Either<Set<E>, E>> optionalCachedResult = uow.cacheLookup(facet.hashCode());
                  if (optionalCachedResult.isPresent()) {
                      uowCacheHits.mark();
                      logger.info("UnitOfWork({}) cache hit for facet: {} with key: {}", uow.hashCode(), facet.toString(), facet.hashCode());
                      Either<Set<E>, E> eitherCachedResult = optionalCachedResult.get();
                      if (eitherCachedResult.isRight()) {
                          E cachedResult = eitherCachedResult.getRight();
                          result = Optional.of(cachedResult);
                      }
                      break;
                  }
              }
          } else {
              // The operation didn't provide enough information to uniquely identify the entity object
              // using one of the facets, but that doesn't mean a filtering query won't return a proper
              // result.  Look in the cache to see if this statement has been executed before.
              stmtKey = getStatementCacheKey();
              Optional<Either<Set<E>, E>> optionalCachedResult = uow.cacheLookup(stmtKey.hashCode());
              if (optionalCachedResult.isPresent()) {
                  Either<Set<E>, E> eitherCachedResult = optionalCachedResult.get();
                  if (eitherCachedResult.isLeft()) {
                      Set<E> cachedResult = eitherCachedResult.getLeft();
                      // Ensure that this non-indexed selection uniquely identified an Entity.
                      if (!(cachedResult.isEmpty() || cachedResult.size() > 1)) {
                          uowCacheHits.mark();
                          logger.info("UnitOfWork({}) cache hit for stmt {} {}", uow.hashCode(), stmtKey,
                                  stmtKey.hashCode());
                          result = cachedResult.stream().findFirst();
                      }
                  }
          }
      }

      if (result == null) {
        uowCacheMiss.mark();
        ResultSet resultSet = execute(sessionOps, uow, traceContext, showValues, true);
        result = transform(resultSet);

        if (enableCache && result.isPresent()) {
            // If we executed a query that didn't depend on an we have a stmtKey for the filters, add that to the cache.
            if (stmtKey != null) {
                Set<Object> set = new HashSet<Object>(1);
                set.add(result.get());
                uow.getCache().put(stmtKey.hashCode(), set);
            }
            // Now insert this entity into the cache for each facet for this entity that we can fully bind.
            E entity = result.get();
            Map<String, EntityIdentifyingFacet> facetMap = Helenus.entity(result.get().getClass()).getIdentityFacets();
            facetMap.forEach((facetName, facet) -> {
                EntityIdentifyingFacet boundFacet = null;
                if (!facet.isFullyBound()) {
                    boundFacet = new EntityIdentifyingFacet(facet);
                    for (HelenusProperty prop : facet.getUnboundEntityProperties()) {
                        Object value = BeanColumnValueProvider.INSTANCE.getColumnValue(entity, -1, prop);
                        if (value == null) { break; }
                        boundFacet.setValueForProperty(prop, value);
                    }
                }
                if (boundFacet != null && boundFacet.isFullyBound()) {
                    uow.getCache().put(boundFacet.hashCode(), Either)
                }
            });
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
