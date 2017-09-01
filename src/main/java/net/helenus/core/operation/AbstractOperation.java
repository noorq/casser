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

import com.datastax.driver.core.ResultSet;
import java.util.concurrent.CompletableFuture;
import net.helenus.core.AbstractSessionOperations;
import net.helenus.core.UnitOfWork;

public abstract class AbstractOperation<E, O extends AbstractOperation<E, O>>
    extends AbstractStatementOperation<E, O> implements OperationsDelegate<E> {

  public abstract E transform(ResultSet resultSet);

  public AbstractCache getCache() {
    return null;
  }

  public boolean cacheable() {
    return false;
  }

  public CacheKey getCacheKey() {
    return null;
  }

  public AbstractOperation(AbstractSessionOperations sessionOperations) {
    super(sessionOperations);
  }

  public PreparedOperation<E> prepare() {
    return new PreparedOperation<E>(prepareStatement(), this);
  }

  public E sync() {
    return Executioner.INSTANCE.<E>sync(sessionOps, null, traceContext, this, showValues);
  }
  public E sync(UnitOfWork uow) {
    return Executioner.INSTANCE.<E>sync(sessionOps, uow, traceContext, this, showValues);
  }

  public CompletableFuture<E> async() {
    AbstractCache cache = getCache();
    boolean cacheResult = cache != null;
    return Executioner.INSTANCE.<E>async(sessionOps, null, traceContext, this, showValues);
  }

  public CompletableFuture<E> async(UnitOfWork uow) {
    return Executioner.INSTANCE.<E>async(sessionOps, uow, traceContext, this, showValues);
  }
}
