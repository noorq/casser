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

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import net.helenus.core.AbstractSessionOperations;
import net.helenus.support.Fun;
import net.helenus.support.Scala;
import scala.concurrent.Future;
import java.util.concurrent.CompletableFuture;
import static net.javacrumbs.futureconverter.java8guava.FutureConverter.*;


public abstract class AbstractOperation<E, O extends AbstractOperation<E, O>> extends AbstractStatementOperation<E, O> {

	public abstract E transform(ResultSet resultSet);

	public boolean cacheable() {
		return false;
	}

	public String getCacheKey() {
		return "";
	}

	public AbstractOperation(AbstractSessionOperations sessionOperations) {
		super(sessionOperations);
	}

	public PreparedOperation<E> prepare() {
		return new PreparedOperation<E>(prepareStatement(), this);
	}

	public ListenableFuture<PreparedOperation<E>> prepareAsync() {

		final O _this = (O) this;

		return Futures.transform(prepareStatementAsync(), new Function<PreparedStatement, PreparedOperation<E>>() {

			@Override
			public PreparedOperation<E> apply(PreparedStatement preparedStatement) {
				return new PreparedOperation<E>(preparedStatement, _this);
			}

		});

	}

	public Future<PreparedOperation<E>> prepareFuture() {
		return Scala.asFuture(prepareAsync());
	}

	public E sync() {

		ResultSet resultSet = sessionOps.executeAsync(options(buildStatement()), showValues).getUninterruptibly();
		E result = transform(resultSet);
		if (cacheable()) {
			sessionOps.cache(getCacheKey(), result);
		}
		return result;
	}

	public ListenableFuture<E> async() {

		ResultSetFuture resultSetFuture = sessionOps.executeAsync(options(buildStatement()), showValues);

		ListenableFuture<E> future = Futures.transform(resultSetFuture, new Function<ResultSet, E>() {

			@Override
			public E apply(ResultSet resultSet) {
				E result = transform(resultSet);
				if (cacheable()) {
					sessionOps.cache(getCacheKey(), result);
				}
				return transform(resultSet);
			}

		}, sessionOps.getExecutor());

		return future;
	}

    public CompletableFuture<E> completable() {
        return toCompletableFuture(async());
    }

	public Future<E> future() {
		return Scala.asFuture(async());
	}

	public <A> Future<Fun.Tuple2<E, A>> future(A a) {
		return Scala.asFuture(async(), a);
	}

	public <A, B> Future<Fun.Tuple3<E, A, B>> future(A a, B b) {
		return Scala.asFuture(async(), a, b);
	}

	public <A, B, C> Future<Fun.Tuple4<E, A, B, C>> future(A a, B b, C c) {
		return Scala.asFuture(async(), a, b, c);
	}

	public <A, B, C, D> Future<Fun.Tuple5<E, A, B, C, D>> future(A a, B b, C c, D d) {
		return Scala.asFuture(async(), a, b, c, d);
	}

}
