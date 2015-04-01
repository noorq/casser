/*
 *      Copyright (C) 2015 Noorq, Inc.
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
package com.noorq.casser.core.operation;

import java.util.stream.Stream;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.noorq.casser.core.AbstractSessionOperations;

public abstract class AbstractStreamOperation<E, O extends AbstractStreamOperation<E, O>> extends AbstractStatementOperation<E, O> {

	public AbstractStreamOperation(AbstractSessionOperations sessionOperations) {
		super(sessionOperations);
	}
	
	public abstract Stream<E> transform(ResultSet resultSet);
	
	public PreparedStreamOperation<E> prepare() {
		return new PreparedStreamOperation<E>(prepareStatement(), this);
	}
	
	public ListenableFuture<PreparedStreamOperation<E>> prepareAsync() {

		final O _this = (O) this;
		
		return Futures.transform(prepareStatementAsync(), new Function<PreparedStatement, PreparedStreamOperation<E>>() {

			@Override
			public PreparedStreamOperation<E> apply(PreparedStatement preparedStatement) {
				return new PreparedStreamOperation<E>(preparedStatement, _this);
			}
			
		});
			
	}
	
	public Stream<E> sync() {
		
		ResultSet resultSet = sessionOps.executeAsync(buildStatement()).getUninterruptibly();

		return transform(resultSet);
	}
	
	public ListenableFuture<Stream<E>> async() {
		
		ResultSetFuture resultSetFuture = sessionOps.executeAsync(buildStatement());

		ListenableFuture<Stream<E>> future = Futures.transform(resultSetFuture, new Function<ResultSet, Stream<E>>() {

			@Override
			public Stream<E> apply(ResultSet resultSet) {
				return transform(resultSet);
			}

		}, sessionOps.getExecutor());
		
		return future;
	}
	
	
}
