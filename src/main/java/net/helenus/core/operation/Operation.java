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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import com.google.common.base.Stopwatch;

import brave.Span;
import brave.Tracer;
import brave.propagation.TraceContext;
import net.helenus.core.AbstractSessionOperations;
import net.helenus.core.UnitOfWork;
import net.helenus.core.cache.Facet;

public abstract class Operation<E> {

	private static final Logger LOG = LoggerFactory.getLogger(Operation.class);

	protected final AbstractSessionOperations sessionOps;
	protected final Meter uowCacheHits;
	protected final Meter uowCacheMiss;
	protected final Meter sessionCacheHits;
	protected final Meter sessionCacheMiss;
	protected final Meter cacheHits;
	protected final Meter cacheMiss;
	protected final Timer requestLatency;

	Operation(AbstractSessionOperations sessionOperations) {
        this.sessionOps = sessionOperations;
        MetricRegistry metrics = sessionOperations.getMetricRegistry();
        if (metrics == null) {
            metrics = new MetricRegistry();
        }
        this.uowCacheHits = metrics.meter("net.helenus.UOW-cache-hits");
        this.uowCacheMiss = metrics.meter("net.helenus.UOW-cache-miss");
        this.sessionCacheHits = metrics.meter("net.helenus.session-cache-hits");
        this.sessionCacheMiss = metrics.meter("net.helenus.session-cache-miss");
        this.cacheHits = metrics.meter("net.helenus.cache-hits");
        this.cacheMiss = metrics.meter("net.helenus.cache-miss");
        this.requestLatency = metrics.timer("net.helenus.request-latency");
    }

	public ResultSet execute(AbstractSessionOperations session, UnitOfWork uow, TraceContext traceContext, long timeout,
			TimeUnit units, boolean showValues, boolean cached) { // throws TimeoutException {

		// Start recording in a Zipkin sub-span our execution time to perform this
		// operation.
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
			Stopwatch timer = Stopwatch.createStarted();
			try {
				ResultSetFuture futureResultSet = session.executeAsync(statement, uow, timer, showValues);
				if (uow != null)
					uow.recordCacheAndDatabaseOperationCount(0, 1);
				ResultSet resultSet = futureResultSet.getUninterruptibly(); // TODO(gburd): (timeout, units);
				return resultSet;

			} finally {
				timer.stop();
				if (uow != null)
					uow.addDatabaseTime("Cassandra", timer);
				log(statement, uow, timer, showValues);
			}

		} finally {

			if (span != null) {
				span.finish();
			}
		}
	}

    public static String queryString(Statement statement, boolean includeValues) {
        String query = null;
        if (statement instanceof BuiltStatement) {
            BuiltStatement builtStatement = (BuiltStatement) statement;
            if (includeValues) {
                RegularStatement regularStatement = builtStatement.setForceNoValues(true);
                query = regularStatement.getQueryString();
            } else {
                    query = builtStatement.getQueryString();
                }
            } else if (statement instanceof RegularStatement) {
                RegularStatement regularStatement = (RegularStatement) statement;
                query = regularStatement.getQueryString();
            } else {
            query = statement.toString();

        }
        return query;
    }

    void log(Statement statement, UnitOfWork uow, Stopwatch timer, boolean showValues) {
		if (LOG.isInfoEnabled()) {
			String uowString = "";
			if (uow != null) {
				uowString = "UOW(" + uow.hashCode() + ")";
			}
            String timerString = "";
			if (timer != null) {
				timerString = String.format(" %s ", timer.toString());
			}
			LOG.info(String.format("%s%s%s",
                    uowString,
                    timerString,
                    Operation.queryString(statement, false)));
		}
	}

	public Statement options(Statement statement) {
		return statement;
	}

	public Statement buildStatement(boolean cached) {
		return null;
	}

	public List<Facet> getFacets() {
		return new ArrayList<Facet>();
	}

	public List<Facet> bindFacetValues() {
		return null;
	}

	public boolean isSessionCacheable() {
		return false;
	}

}
