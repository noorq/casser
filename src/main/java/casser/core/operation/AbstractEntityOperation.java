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

import casser.core.AbstractSessionOperations;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public abstract class AbstractEntityOperation<E, O extends AbstractEntityOperation<E, O>> extends AbstractOperation<E, O> {

	public abstract BuiltStatement buildStatement();
	
	public abstract E transform(ResultSet resultSet);
	
	public AbstractEntityOperation(AbstractSessionOperations sessionOperations) {
		super(sessionOperations);
	}
	
	public E sync() {
		
		ResultSet resultSet = sessionOps.executeAsync(buildStatement()).getUninterruptibly();

		return transform(resultSet);
	}
	
	public ListenableFuture<E> async() {

		ResultSetFuture resultSetFuture = sessionOps.executeAsync(buildStatement());

		ListenableFuture<E> future = Futures.transform(resultSetFuture, new Function<ResultSet, E>() {

			@Override
			public E apply(ResultSet resultSet) {
				return transform(resultSet);
			}

		}, sessionOps.getExecutor());
		
		return future;
	}
	
	
}
