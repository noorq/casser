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
import java.util.Optional;
import java.util.function.Function;

public final class SelectFirstOperation<E>
    extends AbstractFilterOptionalOperation<E, SelectFirstOperation<E>> {

  private final SelectOperation<E> src;

  public SelectFirstOperation(SelectOperation<E> src) {
    super(src.sessionOps);

    this.src = src;
    this.filters = src.filters;
    this.ifFilters = src.ifFilters;
  }

  public <R> SelectFirstTransformingOperation<R, E> map(Function<E, R> fn) {
    return new SelectFirstTransformingOperation<R, E>(src, fn);
  }

  @Override
  public BuiltStatement buildStatement() {
    return src.buildStatement();
  }

  @Override
  public Optional<E> transform(ResultSet resultSet) {
    return src.transform(resultSet).findFirst();
  }
}
