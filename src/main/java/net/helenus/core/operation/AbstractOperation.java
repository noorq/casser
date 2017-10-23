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

import java.util.concurrent.CompletableFuture;

import com.codahale.metrics.Timer;
import com.datastax.driver.core.ResultSet;

import net.helenus.core.AbstractSessionOperations;
import net.helenus.core.UnitOfWork;

public abstract class AbstractOperation<E, O extends AbstractOperation<E, O>> extends AbstractStatementOperation<E, O> {

	public AbstractOperation(AbstractSessionOperations sessionOperations) {
		super(sessionOperations);
	}

	public abstract E transform(ResultSet resultSet);

	public boolean cacheable() {
		return false;
	}

	public PreparedOperation<E> prepare() {
		return new PreparedOperation<E>(prepareStatement(), this);
	}

	public E sync() {// throws TimeoutException {
		final Timer.Context context = requestLatency.time();
		try {
			ResultSet resultSet = this.execute(sessionOps, null, traceContext, queryExecutionTimeout, queryTimeoutUnits,
					showValues, false);
			return transform(resultSet);
		} finally {
			context.stop();
		}
	}

	public E sync(UnitOfWork uow) {// throws TimeoutException {
		if (uow == null)
			return sync();

		final Timer.Context context = requestLatency.time();
		try {
			ResultSet resultSet = execute(sessionOps, uow, traceContext, queryExecutionTimeout, queryTimeoutUnits,
					showValues, true);
			E result = transform(resultSet);
			return result;
		} finally {
			context.stop();
		}
	}

	public CompletableFuture<E> async() {
		return CompletableFuture.<E>supplyAsync(() -> {
			// try {
			return sync();
			// } catch (TimeoutException ex) {
			// throw new CompletionException(ex);
			// }
		});
	}

	public CompletableFuture<E> async(UnitOfWork uow) {
		if (uow == null)
			return async();
		return CompletableFuture.<E>supplyAsync(() -> {
			// try {
			return sync();
			// } catch (TimeoutException ex) {
			// throw new CompletionException(ex);
			// }
		});
	}
}
