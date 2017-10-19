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

import brave.Span;
import brave.Tracer;
import brave.propagation.TraceContext;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import net.helenus.core.AbstractSessionOperations;
import net.helenus.core.UnitOfWork;
import net.helenus.core.cache.BoundFacet;
import net.helenus.core.cache.EntityIdentifyingFacet;

public abstract class Operation<E> {

  protected final AbstractSessionOperations sessionOps;
  protected final Meter uowCacheHits;
  protected final Meter uowCacheMiss;
  protected final Timer requestLatency;

  Operation(AbstractSessionOperations sessionOperations) {
    this.sessionOps = sessionOperations;
    MetricRegistry metrics = sessionOperations.getMetricRegistry();
    this.uowCacheHits = metrics.meter("net.helenus.UOW-cache-hits");
    this.uowCacheMiss = metrics.meter("net.helenus.UOW-cache-miss");
    this.requestLatency = metrics.timer("net.helenus.request-latency");
  }

  public ResultSet execute(
      AbstractSessionOperations session,
      UnitOfWork uow,
      TraceContext traceContext,
      long timeout,
      TimeUnit units,
      boolean showValues,
      boolean cached)
      throws TimeoutException {

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

      Statement statement = options(buildStatement(cached));
      ResultSetFuture futureResultSet = session.executeAsync(statement, showValues);
      return futureResultSet.getUninterruptibly(timeout, units);
    } finally {

      if (span != null) {
        span.finish();
      }
    }
  }

  public Statement options(Statement statement) {
    return statement;
  }

  public Statement buildStatement(boolean cached) {
    return null;
  }

  public String[] getQueryKeys() {
    return null;
  }

  public Map<String, EntityIdentifyingFacet> getIdentifyingFacets() {
    return null;
  }

  public Set<BoundFacet> bindFacetValues() {
    return null;
  }
}
