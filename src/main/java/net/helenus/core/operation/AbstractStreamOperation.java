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
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import net.helenus.core.AbstractSessionOperations;
import net.helenus.core.UnitOfWork;

public abstract class AbstractStreamOperation<E, O extends AbstractStreamOperation<E, O>>
    extends AbstractStatementOperation<E, O> implements OperationsDelegate<Stream<E>> {

  public AbstractStreamOperation(AbstractSessionOperations sessionOperations) {
    super(sessionOperations);
  }

  public abstract Stream<E> transform(ResultSet resultSet);

  protected AbstractCache getCache() {
    return null;
  }

  public CacheKey getCacheKey() {
    return null;
  }

  public PreparedStreamOperation<E> prepare() {
    return new PreparedStreamOperation<E>(prepareStatement(), this);
  }

  public ListenableFuture<PreparedStreamOperation<E>> prepareAsync() {
    final O _this = (O) this;
    return Futures.transform(
        prepareStatementAsync(),
        new Function<PreparedStatement, PreparedStreamOperation<E>>() {
          @Override
          public PreparedStreamOperation<E> apply(PreparedStatement preparedStatement) {
            return new PreparedStreamOperation<E>(preparedStatement, _this);
          }
        });
  }

  public Stream<E> sync() {
    return Executioner.INSTANCE.<Stream<E>>sync(
            sessionOps, null, options(buildStatement()), getCache(), traceContext, this, showValues);
  }

  public Stream<E> sync(UnitOfWork uow) {
    return Executioner.INSTANCE.<Stream<E>>sync(
            sessionOps, uow, options(buildStatement()), getCache(), traceContext, this, showValues);
  }

  public CompletableFuture<Stream<E>> async() {
    return Executioner.INSTANCE.<Stream<E>>async(
            sessionOps, null, options(buildStatement()), getCache(), traceContext, this, showValues);
  }

  public CompletableFuture<Stream<E>> async(UnitOfWork uow) {
    return Executioner.INSTANCE.<Stream<E>>async(
            sessionOps, uow, options(buildStatement()), getCache(), traceContext, this, showValues);
  }
}
