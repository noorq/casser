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
package net.helenus.support;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import scala.concurrent.Future;
import scala.concurrent.impl.Promise.DefaultPromise;

public final class Scala {

	public static <T> Future<T> asFuture(ListenableFuture<T> future) {
		final scala.concurrent.Promise<T> promise = new DefaultPromise<T>();
		Futures.addCallback(future, new FutureCallback<T>() {
			@Override
			public void onSuccess(T result) {
				promise.success(result);
			}
			@Override
			public void onFailure(Throwable t) {
				promise.failure(t);
			}
		});
		return promise.future();
	}

	public static <T, A> Future<Fun.Tuple2<T, A>> asFuture(ListenableFuture<T> future, A a) {
		final scala.concurrent.Promise<Fun.Tuple2<T, A>> promise = new DefaultPromise<Fun.Tuple2<T, A>>();
		Futures.addCallback(future, new FutureCallback<T>() {
			@Override
			public void onSuccess(T result) {
				promise.success(new Fun.Tuple2<T, A>(result, a));
			}
			@Override
			public void onFailure(Throwable t) {
				promise.failure(t);
			}
		});
		return promise.future();
	}

	public static <T, A, B> Future<Fun.Tuple3<T, A, B>> asFuture(ListenableFuture<T> future, A a, B b) {
		final scala.concurrent.Promise<Fun.Tuple3<T, A, B>> promise = new DefaultPromise<Fun.Tuple3<T, A, B>>();
		Futures.addCallback(future, new FutureCallback<T>() {
			@Override
			public void onSuccess(T result) {
				promise.success(new Fun.Tuple3<T, A, B>(result, a, b));
			}
			@Override
			public void onFailure(Throwable t) {
				promise.failure(t);
			}
		});
		return promise.future();
	}

	public static <T, A, B, C> Future<Fun.Tuple4<T, A, B, C>> asFuture(ListenableFuture<T> future, A a, B b, C c) {
		final scala.concurrent.Promise<Fun.Tuple4<T, A, B, C>> promise = new DefaultPromise<Fun.Tuple4<T, A, B, C>>();
		Futures.addCallback(future, new FutureCallback<T>() {
			@Override
			public void onSuccess(T result) {
				promise.success(new Fun.Tuple4<T, A, B, C>(result, a, b, c));
			}
			@Override
			public void onFailure(Throwable t) {
				promise.failure(t);
			}
		});
		return promise.future();
	}

	public static <T, A, B, C, D> Future<Fun.Tuple5<T, A, B, C, D>> asFuture(ListenableFuture<T> future, A a, B b, C c,
			D d) {
		final scala.concurrent.Promise<Fun.Tuple5<T, A, B, C, D>> promise = new DefaultPromise<Fun.Tuple5<T, A, B, C, D>>();
		Futures.addCallback(future, new FutureCallback<T>() {
			@Override
			public void onSuccess(T result) {
				promise.success(new Fun.Tuple5<T, A, B, C, D>(result, a, b, c, d));
			}
			@Override
			public void onFailure(Throwable t) {
				promise.failure(t);
			}
		});
		return promise.future();
	}

}
