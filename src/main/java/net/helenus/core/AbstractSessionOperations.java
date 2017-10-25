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
package net.helenus.core;

import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.ListenableFuture;

import brave.Tracer;
import net.helenus.core.cache.Facet;
import net.helenus.mapping.value.ColumnValuePreparer;
import net.helenus.mapping.value.ColumnValueProvider;
import net.helenus.support.HelenusException;

public abstract class AbstractSessionOperations {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractSessionOperations.class);

	public abstract Session currentSession();

	public abstract String usingKeyspace();

	public abstract boolean isShowCql();

	public abstract PrintStream getPrintStream();

	public abstract Executor getExecutor();

	public abstract SessionRepository getSessionRepository();

	public abstract ColumnValueProvider getValueProvider();

	public abstract ColumnValuePreparer getValuePreparer();

	public abstract ConsistencyLevel getDefaultConsistencyLevel();

	public abstract boolean getDefaultQueryIdempotency();

	public PreparedStatement prepare(RegularStatement statement) {
		try {
			log(statement, null, null, false);
			return currentSession().prepare(statement);
		} catch (RuntimeException e) {
			throw translateException(e);
		}
	}

	public ListenableFuture<PreparedStatement> prepareAsync(RegularStatement statement) {
		try {
			log(statement, null, null, false);
			return currentSession().prepareAsync(statement);
		} catch (RuntimeException e) {
			throw translateException(e);
		}
	}

	public ResultSet execute(Statement statement, boolean showValues) {
		return execute(statement, null, null, showValues);
	}

	public ResultSet execute(Statement statement, Stopwatch timer, boolean showValues) {
		return execute(statement, null, timer, showValues);
	}

	public ResultSet execute(Statement statement, UnitOfWork uow, boolean showValues) {
		return execute(statement, uow, null, showValues);
	}

	public ResultSet execute(Statement statement, UnitOfWork uow, Stopwatch timer, boolean showValues) {
		return executeAsync(statement, uow, timer, showValues).getUninterruptibly();
	}

	public ResultSetFuture executeAsync(Statement statement, boolean showValues) {
		return executeAsync(statement, null, null, showValues);
	}

	public ResultSetFuture executeAsync(Statement statement, Stopwatch timer, boolean showValues) {
		return executeAsync(statement, null, timer, showValues);
	}

	public ResultSetFuture executeAsync(Statement statement, UnitOfWork uow, boolean showValues) {
		return executeAsync(statement, uow, null, showValues);
	}

	public ResultSetFuture executeAsync(Statement statement, UnitOfWork uow, Stopwatch timer, boolean showValues) {
		try {
			log(statement, uow, timer, showValues);
			return currentSession().executeAsync(statement);
		} catch (RuntimeException e) {
			throw translateException(e);
		}
	}

	void log(Statement statement, UnitOfWork uow, Stopwatch timer, boolean showValues) {
		if (LOG.isInfoEnabled()) {
			String timerString = "";
			String uowString = "";
			if (uow != null) {
				uowString = (timer != null) ? " " : "" + "UOW(" + uow.hashCode() + ")";
			}
			if (timer != null) {
				timerString = String.format(" %s", timer.toString());
			}
			LOG.info(String.format("CQL%s%s - %s", uowString, timerString, statement));
		}
		if (isShowCql()) {
			if (statement instanceof BuiltStatement) {
				BuiltStatement builtStatement = (BuiltStatement) statement;
				if (showValues) {
					RegularStatement regularStatement = builtStatement.setForceNoValues(true);
					printCql(regularStatement.getQueryString());
				} else {
					printCql(builtStatement.getQueryString());
				}
			} else if (statement instanceof RegularStatement) {
				RegularStatement regularStatement = (RegularStatement) statement;
				printCql(regularStatement.getQueryString());
			} else {
				printCql(statement.toString());
			}
		}
	}

	public Tracer getZipkinTracer() {
		return null;
	}

	public MetricRegistry getMetricRegistry() {
		return null;
	}

	public void mergeCache(Table<String, String, Object> cache) {
	}

	RuntimeException translateException(RuntimeException e) {
		if (e instanceof HelenusException) {
			return e;
		}
		throw new HelenusException(e);
	}

	public Object checkCache(String tableName, List<Facet> facets) {
		return null;
	}

	public void updateCache(Object pojo, List<Facet> facets) {
	}

	void printCql(String cql) {
		getPrintStream().println(cql);
	}
}
