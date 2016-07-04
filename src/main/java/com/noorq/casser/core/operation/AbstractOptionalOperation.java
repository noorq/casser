/*
 *      Copyright (C) 2015 The Casser Authors
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

import java.util.Optional;

import scala.None;
import scala.Option;
import scala.Some;
import scala.concurrent.Future;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.noorq.casser.core.AbstractSessionOperations;
import com.noorq.casser.support.Fun;
import com.noorq.casser.support.Scala;

public abstract class AbstractOptionalOperation<E, O extends AbstractOptionalOperation<E, O>> extends AbstractStatementOperation<E, O> {

	public AbstractOptionalOperation(AbstractSessionOperations sessionOperations) {
		super(sessionOperations);
	}
	
	public abstract Optional<E> transform(ResultSet resultSet);
	
	public PreparedOptionalOperation<E> prepare() {
		return new PreparedOptionalOperation<E>(prepareStatement(), this);
	}
	
	public ListenableFuture<PreparedOptionalOperation<E>> prepareAsync() {

		final O _this = (O) this;
		
		return Futures.transform(prepareStatementAsync(), new Function<PreparedStatement, PreparedOptionalOperation<E>>() {

			@Override
			public PreparedOptionalOperation<E> apply(PreparedStatement preparedStatement) {
				return new PreparedOptionalOperation<E>(preparedStatement, _this);
			}
			
		});
			
	}
	
	public Future<PreparedOptionalOperation<E>> prepareFuture() {
		return Scala.asFuture(prepareAsync());
	}

	public Optional<E> sync() {
		
		ResultSet resultSet = sessionOps.executeAsync(options(buildStatement()), showValues).getUninterruptibly();

		return transform(resultSet);
	}
	
	public ListenableFuture<Optional<E>> async() {
		
		ResultSetFuture resultSetFuture = sessionOps.executeAsync(options(buildStatement()), showValues);

		ListenableFuture<Optional<E>> future = Futures.transform(resultSetFuture, new Function<ResultSet, Optional<E>>() {

			@Override
			public Optional<E> apply(ResultSet resultSet) {
				return transform(resultSet);
			}

		}, sessionOps.getExecutor());
		
		return future;
	}

	public ListenableFuture<Option<E>> asyncForScala() {
		
		ResultSetFuture resultSetFuture = sessionOps.executeAsync(options(buildStatement()), showValues);

		ListenableFuture<Option<E>> future = Futures.transform(resultSetFuture, new Function<ResultSet, Option<E>>() {

			@Override
			public Option<E> apply(ResultSet resultSet) {
				Optional<E> optional = transform(resultSet);
				if (optional.isPresent()) {
					return new Some<E>(optional.get());
				}
				else {
					return Option.empty();
				}
			}

		}, sessionOps.getExecutor());
		
		return future;
	}
	public Future<Option<E>> future() {
		return Scala.asFuture(asyncForScala());
	}

	public <A> Future<Fun.Tuple2<Option<E>, A>> future(A a) {
		return Scala.asFuture(asyncForScala(), a);
	}

	public <A, B> Future<Fun.Tuple3<Option<E>, A, B>> future(A a, B b) {
		return Scala.asFuture(asyncForScala(), a, b);
	}

	public <A, B, C> Future<Fun.Tuple4<Option<E>, A, B, C>> future(A a, B b, C c) {
		return Scala.asFuture(asyncForScala(), a, b, c);
	}

	public <A, B, C, D> Future<Fun.Tuple5<Option<E>, A, B, C, D>> future(A a, B b, C c, D d) {
		return Scala.asFuture(asyncForScala(), a, b, c, d);
	}
	
}
