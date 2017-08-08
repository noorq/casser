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

import java.util.stream.Stream;

import brave.Span;
import brave.Tracer;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import net.helenus.core.AbstractSessionOperations;

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
		return Futures.transform(prepareStatementAsync(),
				new Function<PreparedStatement, PreparedStreamOperation<E>>() {
					@Override
					public PreparedStreamOperation<E> apply(PreparedStatement preparedStatement) {
						return new PreparedStreamOperation<E>(preparedStatement, _this);
					}
				});
	}

    public Stream<E> sync() {
        Tracer tracer = this.sessionOps.getZipkinTracer();
        final Span cassandraSpan = (tracer != null && span != null) ? tracer.newChild(span.context()) : null;
        if (cassandraSpan != null) {
            cassandraSpan.name("cassandra");
            cassandraSpan.start();
        }

        ResultSet resultSet = sessionOps.executeAsync(options(buildStatement()), showValues).getUninterruptibly();
		Stream<E> result = transform(resultSet);

        if (cassandraSpan != null) {
            cassandraSpan.finish();
        }

        return result;
	}

	public ListenableFuture<Stream<E>> async() {
        Tracer tracer = this.sessionOps.getZipkinTracer();
        final Span cassandraSpan = (tracer != null && span != null) ? tracer.newChild(span.context()) : null;
        if (cassandraSpan != null) {
            cassandraSpan.name("cassandra");
            cassandraSpan.start();
        }

		ResultSetFuture resultSetFuture = sessionOps.executeAsync(options(buildStatement()), showValues);
		ListenableFuture<Stream<E>> future = Futures.transform(resultSetFuture,
                new Function<ResultSet, Stream<E>>() {
                    @Override
                    public Stream<E> apply(ResultSet resultSet) {
                        Stream<E> result = transform(resultSet);
                        if (cassandraSpan != null) {
                            cassandraSpan.finish();
                        }
                        return result;
                    }
                }, sessionOps.getExecutor());
		return future;
	}

}
