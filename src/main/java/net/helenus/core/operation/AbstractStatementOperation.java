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

import brave.Tracer;
import brave.propagation.TraceContext;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import net.helenus.core.AbstractSessionOperations;
import net.helenus.core.UnitOfWork;
import net.helenus.core.cache.BoundFacet;
import net.helenus.core.cache.EntityIdentifyingFacet;
import net.helenus.core.reflect.MapExportable;
import net.helenus.mapping.value.BeanColumnValueProvider;
import net.helenus.support.Either;
import net.helenus.support.HelenusException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractStatementOperation<E, O extends AbstractStatementOperation<E, O>>
    extends Operation<E> {

  final Logger logger = LoggerFactory.getLogger(getClass());
  protected boolean enableCache = true;
  protected boolean showValues = true;
  protected TraceContext traceContext;
  long queryExecutionTimeout = 10;
  TimeUnit queryTimeoutUnits = TimeUnit.SECONDS;
  private ConsistencyLevel consistencyLevel;
  private ConsistencyLevel serialConsistencyLevel;
  private RetryPolicy retryPolicy;
  private boolean idempotent = false;
  private boolean enableTracing = false;
  private long[] defaultTimestamp = null;
  private int[] fetchSize = null;

  public AbstractStatementOperation(AbstractSessionOperations sessionOperations) {
    super(sessionOperations);
    this.consistencyLevel = sessionOperations.getDefaultConsistencyLevel();
    this.idempotent = sessionOperations.getDefaultQueryIdempotency();
  }

  public abstract Statement buildStatement(boolean cached);

  public O ignoreCache(boolean enabled) {
    enableCache = enabled;
    return (O) this;
  }

  public O ignoreCache() {
    enableCache = true;
    return (O) this;
  }

  public O showValues(boolean enabled) {
    this.showValues = enabled;
    return (O) this;
  }

  public O defaultTimestamp(long timestamp) {
    this.defaultTimestamp = new long[1];
    this.defaultTimestamp[0] = timestamp;
    return (O) this;
  }

  public O retryPolicy(RetryPolicy retryPolicy) {
    this.retryPolicy = retryPolicy;
    return (O) this;
  }

  public O defaultRetryPolicy() {
    this.retryPolicy = DefaultRetryPolicy.INSTANCE;
    return (O) this;
  }

  public O idempotent() {
    this.idempotent = true;
    return (O) this;
  }

  public O isIdempotent(boolean idempotent) {
    this.idempotent = idempotent;
    return (O) this;
  }

  public O downgradingConsistencyRetryPolicy() {
    this.retryPolicy = DowngradingConsistencyRetryPolicy.INSTANCE;
    return (O) this;
  }

  public O fallthroughRetryPolicy() {
    this.retryPolicy = FallthroughRetryPolicy.INSTANCE;
    return (O) this;
  }

  public O consistency(ConsistencyLevel level) {
    this.consistencyLevel = level;
    return (O) this;
  }

  public O consistencyAny() {
    this.consistencyLevel = ConsistencyLevel.ANY;
    return (O) this;
  }

  public O consistencyOne() {
    this.consistencyLevel = ConsistencyLevel.ONE;
    return (O) this;
  }

  public O consistencyQuorum() {
    this.consistencyLevel = ConsistencyLevel.QUORUM;
    return (O) this;
  }

  public O consistencyAll() {
    this.consistencyLevel = ConsistencyLevel.ALL;
    return (O) this;
  }

  public O consistencyLocalOne() {
    this.consistencyLevel = ConsistencyLevel.LOCAL_ONE;
    return (O) this;
  }

  public O consistencyLocalQuorum() {
    this.consistencyLevel = ConsistencyLevel.LOCAL_QUORUM;
    return (O) this;
  }

  public O consistencyEachQuorum() {
    this.consistencyLevel = ConsistencyLevel.EACH_QUORUM;
    return (O) this;
  }

  public O serialConsistency(ConsistencyLevel level) {
    this.serialConsistencyLevel = level;
    return (O) this;
  }

  public O serialConsistencyAny() {
    this.serialConsistencyLevel = ConsistencyLevel.ANY;
    return (O) this;
  }

  public O serialConsistencyOne() {
    this.serialConsistencyLevel = ConsistencyLevel.ONE;
    return (O) this;
  }

  public O serialConsistencyQuorum() {
    this.serialConsistencyLevel = ConsistencyLevel.QUORUM;
    return (O) this;
  }

  public O serialConsistencyAll() {
    this.serialConsistencyLevel = ConsistencyLevel.ALL;
    return (O) this;
  }

  public O serialConsistencyLocal() {
    this.serialConsistencyLevel = ConsistencyLevel.LOCAL_SERIAL;
    return (O) this;
  }

  public O serialConsistencyLocalQuorum() {
    this.serialConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM;
    return (O) this;
  }

  public O disableTracing() {
    this.enableTracing = false;
    return (O) this;
  }

  public O enableTracing() {
    this.enableTracing = true;
    return (O) this;
  }

  public O tracing(boolean enable) {
    this.enableTracing = enable;
    return (O) this;
  }

  public O fetchSize(int fetchSize) {
    this.fetchSize = new int[1];
    this.fetchSize[0] = fetchSize;
    return (O) this;
  }

  public O queryTimeoutMs(long ms) {
    this.queryExecutionTimeout = ms;
    this.queryTimeoutUnits = TimeUnit.MILLISECONDS;
    return (O) this;
  }

  public O queryTimeout(long timeout, TimeUnit units) {
    this.queryExecutionTimeout = timeout;
    this.queryTimeoutUnits = units;
    return (O) this;
  }

  public Statement options(Statement statement) {

    if (defaultTimestamp != null) {
      statement.setDefaultTimestamp(defaultTimestamp[0]);
    }

    if (consistencyLevel != null) {
      statement.setConsistencyLevel(consistencyLevel);
    }

    if (serialConsistencyLevel != null) {
      statement.setSerialConsistencyLevel(serialConsistencyLevel);
    }

    if (retryPolicy != null) {
      statement.setRetryPolicy(retryPolicy);
    }

    if (enableTracing) {
      statement.enableTracing();
    } else {
      statement.disableTracing();
    }

    if (fetchSize != null) {
      statement.setFetchSize(fetchSize[0]);
    }

    if (idempotent) {
      statement.setIdempotent(true);
    }

    return statement;
  }

  public O zipkinContext(TraceContext traceContext) {
    if (traceContext != null) {
      Tracer tracer = this.sessionOps.getZipkinTracer();
      if (tracer != null) {
        this.traceContext = traceContext;
      }
    }

    return (O) this;
  }

  public Statement statement() {
    return buildStatement(false);
  }

  public String cql() {
    Statement statement = buildStatement(false);
    if (statement == null) return "";
    if (statement instanceof BuiltStatement) {
      BuiltStatement buildStatement = (BuiltStatement) statement;
      return buildStatement.setForceNoValues(true).getQueryString();
    } else {
      return statement.toString();
    }
  }

  public PreparedStatement prepareStatement() {

    Statement statement = buildStatement(true);

    if (statement instanceof RegularStatement) {

      RegularStatement regularStatement = (RegularStatement) statement;

      return sessionOps.prepare(regularStatement);
    }

    throw new HelenusException("only RegularStatements can be prepared");
  }

  public ListenableFuture<PreparedStatement> prepareStatementAsync() {

    Statement statement = buildStatement(true);

    if (statement instanceof RegularStatement) {

      RegularStatement regularStatement = (RegularStatement) statement;

      return sessionOps.prepareAsync(regularStatement);
    }

    throw new HelenusException("only RegularStatements can be prepared");
  }

  protected E checkCache(UnitOfWork<?> uow, Set<BoundFacet> facets, String[] statementKeys) {
    E result = null;
    Optional<Either<Object, Set<Object>>> optionalCachedResult = Optional.empty();

    if (!facets.isEmpty()) {
      //TODO(gburd): what about select ResultSet, Tuple... etc.?
      optionalCachedResult = uow.cacheLookupByFacet(facets);
      if (optionalCachedResult.isPresent()) {
        Either<Object, Set<Object>> eitherCachedResult = optionalCachedResult.get();
        if (eitherCachedResult.isLeft()) {
          uowCacheHits.mark();
          logger.info("UnitOfWork({}) cache hit using facets", uow.hashCode());
          result = (E) eitherCachedResult.getLeft();
        }
      }
    }

    if (result == null && statementKeys != null) {
      // Then check to see if this query happens to uniquely identify a single object in thecache.
      optionalCachedResult = uow.cacheLookupByStatement(statementKeys);
      if (optionalCachedResult.isPresent()) {
        Either<Object, Set<Object>> eitherCachedResult = optionalCachedResult.get();
        // Statements always store Set<E> as the value in the cache.
        if (eitherCachedResult.isRight()) {
          Set<Object> cachedResult = eitherCachedResult.getRight();
          if (cachedResult.size() == 1) {
            Optional<Object> maybeResult = cachedResult.stream().findFirst();
            if (maybeResult.isPresent()) {
              uowCacheHits.mark();
              logger.info("UnitOfWork({}) cache hit for stmt", uow.hashCode());
            } else {
              result = null;
            }
          }
        }
      }
    }

    if (result == null) {
      uowCacheMiss.mark();
      logger.info("UnitOfWork({}) cache miss", uow.hashCode());
    }

    return result;
  }

  protected void updateCache(
      UnitOfWork<?> uow,
      E pojo,
      Map<String, EntityIdentifyingFacet> facetMap,
      String[] statementKeys) {

    // Insert this entity into the cache for each facet for this entity that we can fully bind.
    Map<String, BoundFacet> boundFacets = new HashMap<String, BoundFacet>();
    Map<String, Object> valueMap =
        pojo instanceof MapExportable ? ((MapExportable) pojo).toMap() : null;
    facetMap.forEach(
        (facetName, facet) -> {
          if (!facet.isFullyBound()) {
            EntityIdentifyingFacet.Binder binder = facet.binder();
            facet
                .getProperties()
                .forEach(
                    prop -> {
                      if (valueMap == null) {
                        Object value =
                            BeanColumnValueProvider.INSTANCE.getColumnValue(pojo, -1, prop, false);
                        binder.setValueForProperty(
                            prop, prop.getColumnName().toCql() + "==" + value.toString());
                      } else {
                        binder.setValueForProperty(
                            prop,
                            prop.getColumnName().toCql()
                                + "=="
                                + valueMap.get(prop.getPropertyName()).toString());
                      }
                    });
            boundFacets.put(facetName, binder.bind());
          }
        });

    // Cache the value (pojo), the statement key, and the fully bound facets.
    uow.cacheUpdate(Either.left(pojo), statementKeys, boundFacets);
  }
}
