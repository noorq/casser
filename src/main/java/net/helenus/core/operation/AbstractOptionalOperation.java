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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import net.helenus.core.AbstractSessionOperations;

public abstract class AbstractOptionalOperation<E, O extends AbstractOptionalOperation<E, O>>
    extends AbstractStatementOperation<E, O> implements Transformational<Optional<E>> {

  public AbstractOptionalOperation(AbstractSessionOperations sessionOperations) {
    super(sessionOperations);
  }

  public abstract Optional<E> transform(ResultSet resultSet);

  public PreparedOptionalOperation<E> prepare() {
    return new PreparedOptionalOperation<E>(prepareStatement(), this);
  }

  public ListenableFuture<PreparedOptionalOperation<E>> prepareAsync() {
    final O _this = (O) this;
    return Futures.transform(
        prepareStatementAsync(),
        new Function<PreparedStatement, PreparedOptionalOperation<E>>() {
          @Override
          public PreparedOptionalOperation<E> apply(PreparedStatement preparedStatement) {
            return new PreparedOptionalOperation<E>(preparedStatement, _this);
          }
        });
  }

  public Optional<E> sync() {

    return Executioner.INSTANCE.<Optional<E>>sync(
        sessionOps, options(buildStatement()), traceContext, this, showValues);
  }

  public CompletableFuture<Optional<E>> async() {
    return Executioner.INSTANCE.<Optional<E>>async(
        sessionOps, options(buildStatement()), traceContext, this, showValues);
  }
}
