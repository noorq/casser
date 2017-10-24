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
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import com.codahale.metrics.Timer;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import net.helenus.core.AbstractSessionOperations;
import net.helenus.core.UnitOfWork;
import net.helenus.core.cache.CacheUtil;
import net.helenus.core.cache.Facet;

public abstract class AbstractStreamOperation<E, O extends AbstractStreamOperation<E, O>>
		extends
			AbstractStatementOperation<E, O> {

	public AbstractStreamOperation(AbstractSessionOperations sessionOperations) {
		super(sessionOperations);
	}

	public abstract Stream<E> transform(ResultSet resultSet);

	public PreparedStreamOperation<E> prepare() {
		return new PreparedStreamOperation<E>(prepareStatement(), this);
	}

	public ListenableFuture<PreparedStreamOperation<E>> prepareAsync() {
		final O _this = (O) this;
		return Futures.transform(prepareStatementAsync(),
				new Function<PreparedStatement, PreparedStreamOperation<E>>() {
					@Override
					public PreparedStreamOperation<E> apply(PreparedStatement preparedStatement) {
						return new PreparedStreamOperation<E>(preparedStatement, _this);
					}
				});
	}

	public Stream<E> sync() {// throws TimeoutException {
		final Timer.Context context = requestLatency.time();
		try {
			Stream<E> resultStream = null;
			E cacheResult = null;
			boolean updateCache = isSessionCacheable();

			if (enableCache && isSessionCacheable()) {
				List<Facet> facets = bindFacetValues();
				String tableName = CacheUtil.schemaName(facets);
				cacheResult = (E) sessionOps.checkCache(tableName, facets);
				if (cacheResult != null) {
					resultStream = Stream.of(cacheResult);
					updateCache = false;
					sessionCacheHits.mark();
					cacheHits.mark();
				} else {
				    sessionCacheMiss.mark();
				    cacheMiss.mark();
                }
			}

			if (resultStream == null) {
				// Formulate the query and execute it against the Cassandra cluster.
				ResultSet resultSet = this.execute(sessionOps, null, traceContext, queryExecutionTimeout,
						queryTimeoutUnits, showValues, false);

				// Transform the query result set into the desired shape.
				resultStream = transform(resultSet);
			}

			if (updateCache && resultStream != null) {
				List<Facet> facets = getFacets();
				if (facets != null && facets.size() > 1) {
					List<E> again = new ArrayList<>();
					resultStream.forEach(result -> {
						sessionOps.updateCache(result, facets);
						again.add(result);
					});
					resultStream = again.stream();
				}
			}
			return resultStream;

		} finally {
			context.stop();
		}
	}

	public Stream<E> sync(UnitOfWork<?> uow) {// throws TimeoutException {
		if (uow == null)
			return sync();

		final Timer.Context context = requestLatency.time();
		try {
			Stream<E> resultStream = null;
			E cachedResult = null;
			boolean updateCache = true;

			if (enableCache) {
				Stopwatch timer = Stopwatch.createStarted();
				try {
                    List<Facet> facets = bindFacetValues();
                    cachedResult = checkCache(uow, facets);
                    if (cachedResult != null) {
                        resultStream = Stream.of(cachedResult);
                        updateCache = false;
                        uowCacheHits.mark();
                        cacheHits.mark();
                        uow.recordCacheAndDatabaseOperationCount(1, 0);
                    } else {
                        uowCacheMiss.mark();
                        if (isSessionCacheable()) {
                            String tableName = CacheUtil.schemaName(facets);
                            cachedResult = (E) sessionOps.checkCache(tableName, facets);
                            if (cachedResult != null) {
                                resultStream = Stream.of(cachedResult);
                                sessionCacheHits.mark();
                                cacheHits.mark();
                                uow.recordCacheAndDatabaseOperationCount(1, 0);
                            } else {
                                sessionCacheMiss.mark();
                                cacheMiss.mark();
                                uow.recordCacheAndDatabaseOperationCount(-1, 0);
                            }
                        }
                    }
                } finally {
                    timer.stop();
                    uow.addCacheLookupTime(timer);
                }
			}

			if (resultStream == null) {
				ResultSet resultSet = execute(sessionOps, uow, traceContext, queryExecutionTimeout, queryTimeoutUnits,
						showValues, true);
				resultStream = transform(resultSet);
			}

			// If we have a result and we're caching then we need to put it into the cache
			// for future requests to find.
			if (updateCache && resultStream != null) {
				List<E> again = new ArrayList<>();
				List<Facet> facets = getFacets();
				resultStream.forEach(result -> {
					updateCache(uow, result, facets);
					again.add(result);
				});
				resultStream = again.stream();
			}

			return resultStream;
		} finally {
			context.stop();
		}
	}

	public CompletableFuture<Stream<E>> async() {
		return CompletableFuture.<Stream<E>>supplyAsync(() -> {
			// try {
			return sync();
			// } catch (TimeoutException ex) {
			// throw new CompletionException(ex);
			// }
		});
	}

	public CompletableFuture<Stream<E>> async(UnitOfWork<?> uow) {
		if (uow == null)
			return async();
		return CompletableFuture.<Stream<E>>supplyAsync(() -> {
			// try {
			return sync();
			// } catch (TimeoutException ex) {
			// throw new CompletionException(ex);
			// }
		});
	}
}
