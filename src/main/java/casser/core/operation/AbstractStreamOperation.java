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
package casser.core.operation;

import java.util.stream.Stream;

import casser.core.AbstractSessionOperations;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public abstract class AbstractStreamOperation<E, O extends AbstractStreamOperation<E, O>> extends AbstractOperation<E, O> {

	public AbstractStreamOperation(AbstractSessionOperations sessionOperations) {
		super(sessionOperations);
	}
	
	public abstract Stream<E> transform(ResultSet resultSet);
	
	public Stream<E> sync() {
		
		ResultSet resultSet = sessionOperations.executeAsync(buildStatement()).getUninterruptibly();

		return transform(resultSet);
	}
	
	public ListenableFuture<Stream<E>> async() {
		
		ResultSetFuture resultSetFuture = sessionOperations.executeAsync(buildStatement());

		ListenableFuture<Stream<E>> future = Futures.transform(resultSetFuture, new Function<ResultSet, Stream<E>>() {

			@Override
			public Stream<E> apply(ResultSet resultSet) {
				return transform(resultSet);
			}

		}, sessionOperations.getExecutor());
		
		return future;
	}
	
	
}
