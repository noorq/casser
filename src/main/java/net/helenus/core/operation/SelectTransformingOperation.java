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
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import net.helenus.core.cache.Facet;

public final class SelectTransformingOperation<R, E>
    extends AbstractFilterStreamOperation<R, SelectTransformingOperation<R, E>> {

  private final SelectOperation<E> delegate;
  private final Function<E, R> fn;

  public SelectTransformingOperation(SelectOperation<E> delegate, Function<E, R> fn) {
    super(delegate.sessionOps);

    this.delegate = delegate;
    this.fn = fn;
    this.filters = delegate.filters;
    this.ifFilters = delegate.ifFilters;
  }

  @Override
  public List<Facet> bindFacetValues() {
    return delegate.bindFacetValues();
  }

  @Override
  public List<Facet> getFacets() {
    return delegate.getFacets();
  }

  @Override
  public BuiltStatement buildStatement(boolean cached) {
    return delegate.buildStatement(cached);
  }

  @Override
  public Stream<R> transform(ResultSet resultSet) {
    return delegate.transform(resultSet).map(fn);
  }

  @Override
  public boolean isSessionCacheable() {
    return delegate.isSessionCacheable();
  }

  @Override
  public boolean ignoreCache() {
    return delegate.ignoreCache();
  }
}
