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
package net.helenus.mapping.convert.tuple;

import com.datastax.driver.core.TupleValue;
import java.util.List;
import java.util.function.Function;
import net.helenus.core.SessionRepository;
import net.helenus.mapping.convert.ProxyValueReader;
import net.helenus.mapping.value.TupleColumnValueProvider;
import net.helenus.support.Transformers;

public final class TupleListToListConverter implements Function<Object, Object> {

  final ProxyValueReader<TupleValue> reader;

  public TupleListToListConverter(Class<?> iface, SessionRepository repository) {
    this.reader = new ProxyValueReader<TupleValue>(iface, new TupleColumnValueProvider(repository));
  }

  @Override
  public Object apply(Object t) {
    return Transformers.transformList((List<TupleValue>) t, reader);
  }
}
