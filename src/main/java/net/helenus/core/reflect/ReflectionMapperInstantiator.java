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
package net.helenus.core.reflect;

import java.io.Serializable;
import java.lang.reflect.Proxy;
import java.util.Map;
import net.helenus.core.MapperInstantiator;

public enum ReflectionMapperInstantiator implements MapperInstantiator {
  INSTANCE;

  @Override
  @SuppressWarnings("unchecked")
  public <E> E instantiate(Class<E> iface, Map<String, Object> src, ClassLoader classLoader) {

    MapperInvocationHandler<E> handler = new MapperInvocationHandler<E>(iface, src);
    E proxy =
        (E)
            Proxy.newProxyInstance(
                classLoader, new Class[] {iface, MapExportable.class, Serializable.class}, handler);
    return proxy;
  }
}
