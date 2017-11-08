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

import net.helenus.core.Getter;
import net.helenus.core.Helenus;
import net.helenus.core.cache.CacheUtil;
import net.helenus.mapping.MappingUtil;
import net.helenus.mapping.annotation.Transient;
import net.helenus.mapping.value.ValueProviderMap;
import net.helenus.support.HelenusException;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MapperInvocationHandler<E> implements InvocationHandler, Serializable {
  private static final long serialVersionUID = -7044209982830584984L;

  private Map<String, Object> src;
  private final Class<E> iface;

  public MapperInvocationHandler(Class<E> iface, Map<String, Object> src) {
    this.src = src;
    this.iface = iface;
  }

  private Object invokeDefault(Object proxy, Method method, Object[] args) throws Throwable {
    // NOTE: This is reflection magic to invoke (non-recursively) a default method
    // implemented on an interface
    // that we've proxied (in ReflectionDslInstantiator). I found the answer in this
    // article.
    // https://zeroturnaround.com/rebellabs/recognize-and-conquer-java-proxies-default-methods-and-method-handles/

    // First, we need an instance of a private inner-class found in MethodHandles.
    Constructor<MethodHandles.Lookup> constructor =
        MethodHandles.Lookup.class.getDeclaredConstructor(Class.class, int.class);
    constructor.setAccessible(true);

    // Now we need to lookup and invoke special the default method on the interface
    // class.
    final Class<?> declaringClass = method.getDeclaringClass();
    Object result =
        constructor
            .newInstance(declaringClass, MethodHandles.Lookup.PRIVATE)
            .unreflectSpecial(method, declaringClass)
            .bindTo(proxy)
            .invokeWithArguments(args);
    return result;
  }

  private Object writeReplace() {
    return new SerializationProxy<E>(this);
  }

  private void readObject(ObjectInputStream stream) throws InvalidObjectException {
    throw new InvalidObjectException("Proxy required.");
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

    // Transient, default methods should simply be invoked as-is.
    if (method.isDefault() && method.getDeclaredAnnotation(Transient.class) != null) {
      return invokeDefault(proxy, method, args);
    }

    String methodName = method.getName();

    if ("equals".equals(methodName) && method.getParameterCount() == 1) {
      Object otherObj = args[0];
      if (otherObj == null) {
        return false;
      }
      if (Proxy.isProxyClass(otherObj.getClass())) {
        if (this == Proxy.getInvocationHandler(otherObj)) {
          return true;
        }
      }
      if (otherObj instanceof MapExportable) {
        return MappingUtil.compareMaps((MapExportable)otherObj, src);
      }
      return false;
    }

    if (MapExportable.PUT_METHOD.equals(methodName) && method.getParameterCount() == 2) {
      final String key = (String)args[0];
      final Object value = (Object)args[1];
      if (src instanceof ValueProviderMap) {
        this.src = fromValueProviderMap(src);
      }
      src.put(key, value);
      return null;
    }

    if (Entity.WRITTEN_AT_METHOD.equals(methodName) && method.getParameterCount() == 1) {
      final String key;
      if (args[0] instanceof String) {
        key = CacheUtil.writeTimeKey((String)args[0]);
      } else if (args[0] instanceof Getter) {
        Getter getter = (Getter)args[0];
        key = CacheUtil.writeTimeKey(MappingUtil.resolveMappingProperty(getter).getProperty().getPropertyName());
      } else {
        return 0L;
      }
      long[] v = (long[])src.get(key);
      if (v != null) {
        return v[0];
      }
      return 0L;
    }

    if (Entity.TTL_OF_METHOD.equals(methodName) && method.getParameterCount() == 1) {
      final String key;
      if (args[0] instanceof String) {
        key = CacheUtil.ttlKey((String)args[0]);
      } else if (args[0] instanceof Getter) {
        Getter getter = (Getter)args[0];
        key = CacheUtil.ttlKey(MappingUtil.resolveMappingProperty(getter).getProperty().getColumnName().toCql(true));
      } else {
        return 0;
      }
      int v[] = (int[])src.get(key);
      if (v != null) {
        return v[0];
      }
      return 0;
    }

    if (method.getParameterCount() != 0 || method.getReturnType() == void.class) {
      throw new HelenusException("invalid getter method " + method);
    }

    if ("hashCode".equals(methodName)) {
      return hashCode();
    }

    if ("toString".equals(methodName)) {
      return iface.getSimpleName() + ": " + src.toString();
    }

    if ("writeReplace".equals(methodName)) {
      return new SerializationProxy(this);
    }

    if ("readObject".equals(methodName)) {
      throw new InvalidObjectException("Proxy required.");
    }

    if ("dsl".equals(methodName)) {
      return Helenus.dsl(iface);
    }

    if (MapExportable.TO_MAP_METHOD.equals(methodName)) {
      if (method.getParameterCount() == 1 && args[0] instanceof Boolean) {
        if ((boolean)args[0] == true) { return src; }
      }
      return Collections.unmodifiableMap(src);
    }

    final Object value = src.get(methodName);

    if (value == null) {

      Class<?> returnType = method.getReturnType();

      // Default implementations of non-Transient methods in entities are the default
      // value when the map contains 'null'.
      if (method.isDefault()) {
        return invokeDefault(proxy, method, args);
      }

      // Otherwise, if the return type of the method is a primitive Java type then
      // we'll return the standard default values to avoid a NPE in user code.
      if (returnType.isPrimitive()) {
        DefaultPrimitiveTypes type = DefaultPrimitiveTypes.lookup(returnType);
        if (type == null) {
          throw new HelenusException("unknown primitive type " + returnType);
        }
        return type.getDefaultValue();
      }
    }

    return value;
  }

  static Map<String, Object> fromValueProviderMap(Map v) {
    Map<String, Object> m = new HashMap<String, Object>(v.size());
    Set<String> keys = v.keySet();
    for (String key : keys) {
      m.put(key, v.get(key));
    }
    return m;
  }

  static class SerializationProxy<E> implements Serializable {

    private static final long serialVersionUID = -5617583940055969353L;

    private final Class<E> iface;
    private final Map<String, Object> src;

    public SerializationProxy(MapperInvocationHandler mapper) {
      this.iface = mapper.iface;
      if (mapper.src instanceof ValueProviderMap) {
        this.src = fromValueProviderMap(mapper.src);
      } else {
        this.src = mapper.src;
      }
    }

    Object readResolve() throws ObjectStreamException {
      return new MapperInvocationHandler(iface, src);
    }
  }
}
