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
import com.datastax.driver.core.querybuilder.BuiltStatement;
import net.helenus.core.cache.EntityIdentifyingFacet;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public final class SelectFirstOperation<E>
    extends AbstractFilterOptionalOperation<E, SelectFirstOperation<E>> {

  private final SelectOperation<E> delegate;

  public SelectFirstOperation(SelectOperation<E> delegate) {
    super(delegate.sessionOps);

    this.delegate = delegate;
    this.filters = delegate.filters;
    this.ifFilters = delegate.ifFilters;
  }

  public <R> SelectFirstTransformingOperation<R, E> map(Function<E, R> fn) {
    return new SelectFirstTransformingOperation<R, E>(delegate, fn);
  }

  @Override
  public String[] getQueryKeys() {
    return delegate.getQueryKeys();
  }

  @Override
  public BuiltStatement buildStatement(boolean cached) {
    return delegate.buildStatement(cached);
  }

  @Override
  public Set<EntityIdentifyingFacet> getFacets() { return delegate.getFacets(); }

  @Override
  public Optional<E> transform(ResultSet resultSet) {
    return delegate.transform(resultSet).findFirst();
  }
}
