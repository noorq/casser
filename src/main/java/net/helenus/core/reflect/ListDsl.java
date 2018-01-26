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

import java.util.*;
import net.helenus.mapping.HelenusProperty;
import net.helenus.support.DslPropertyException;
import net.helenus.support.HelenusMappingException;

public final class ListDsl<V> implements List<V> {

  private final HelenusPropertyNode parent;

  public ListDsl(HelenusPropertyNode parent) {
    this.parent = parent;
  }

  public HelenusPropertyNode getParent() {
    return parent;
  }

  @Override
  public V get(int index) {
    HelenusProperty prop = new HelenusNamedProperty(Integer.toString(index));
    throw new DslPropertyException(new HelenusPropertyNode(prop, Optional.of(parent)));
  }

  @Override
  public int size() {
    throwShouldNeverCall();
    return 0;
  }

  @Override
  public boolean isEmpty() {
    throwShouldNeverCall();
    return false;
  }

  @Override
  public boolean contains(Object o) {
    throwShouldNeverCall();
    return false;
  }

  @Override
  public Iterator<V> iterator() {
    throwShouldNeverCall();
    return null;
  }

  @Override
  public Object[] toArray() {
    throwShouldNeverCall();
    return null;
  }

  @Override
  public <T> T[] toArray(T[] a) {
    throwShouldNeverCall();
    return null;
  }

  @Override
  public boolean add(V e) {
    throwShouldNeverCall();
    return false;
  }

  @Override
  public boolean remove(Object o) {
    throwShouldNeverCall();
    return false;
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    throwShouldNeverCall();
    return false;
  }

  @Override
  public boolean addAll(Collection<? extends V> c) {
    throwShouldNeverCall();
    return false;
  }

  @Override
  public boolean addAll(int index, Collection<? extends V> c) {
    throwShouldNeverCall();
    return false;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throwShouldNeverCall();
    return false;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throwShouldNeverCall();
    return false;
  }

  @Override
  public void clear() {
    throwShouldNeverCall();
  }

  @Override
  public V set(int index, V element) {
    throwShouldNeverCall();
    return null;
  }

  @Override
  public void add(int index, V element) {
    throwShouldNeverCall();
  }

  @Override
  public V remove(int index) {
    throwShouldNeverCall();
    return null;
  }

  @Override
  public int indexOf(Object o) {
    throwShouldNeverCall();
    return 0;
  }

  @Override
  public int lastIndexOf(Object o) {
    throwShouldNeverCall();
    return 0;
  }

  @Override
  public ListIterator<V> listIterator() {
    throwShouldNeverCall();
    return null;
  }

  @Override
  public ListIterator<V> listIterator(int index) {
    throwShouldNeverCall();
    return null;
  }

  @Override
  public List<V> subList(int fromIndex, int toIndex) {
    throwShouldNeverCall();
    return null;
  }

  private void throwShouldNeverCall() {
    throw new HelenusMappingException("should be never called");
  }

  @Override
  public String toString() {
    return "ListDsl";
  }
}
