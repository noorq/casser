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
package net.helenus.support;

import java.util.*;

public final class Immutables {

	private Immutables() {
	}

	public static <T> Set<T> setOf(T value) {
		return new SingleEntrySet<T>(value);
	}

	public static <T> List<T> listOf(T value) {
		return new SingleEntryList<T>(value);
	}

	public static <K, V> Map<K, V> mapOf(K key, V value) {
		return new SingleEntryMap<K, V>(key, value);
	}

	static class SingleEntryIterator<T> implements Iterator<T> {

		T entry;
		boolean processed = false;

		SingleEntryIterator(T entry) {
			this.entry = entry;
		}

		@Override
		public boolean hasNext() {
			return !processed;
		}

		@Override
		public T next() {
			processed = true;
			return entry;
		}
	}

	static class SingleEntryListIterator<T> extends SingleEntryIterator<T> implements ListIterator<T> {

		SingleEntryListIterator(T entry) {
			super(entry);
		}

		@Override
		public boolean hasPrevious() {
			return processed;
		}

		@Override
		public T previous() {
			processed = false;
			return entry;
		}

		@Override
		public int nextIndex() {
			return processed ? 1 : 0;
		}

		@Override
		public int previousIndex() {
			return processed ? 0 : -1;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void set(T e) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void add(T e) {
			throw new UnsupportedOperationException();
		}
	}

	static class SingleEntryCollection<T> implements Collection<T> {

		final T entry;

		SingleEntryCollection(T entry) {
			this.entry = entry;
		}

		@Override
		public int size() {
			return 1;
		}

		@Override
		public boolean isEmpty() {
			return false;
		}

		@Override
		public boolean contains(Object o) {
			if (entry == null) {
				return o == null;
			}
			return entry.equals(o);
		}

		@Override
		public Iterator<T> iterator() {
			return new SingleEntryIterator<T>(entry);
		}

		@Override
		public Object[] toArray() {
			return new Object[]{entry};
		}

		@Override
		public <V> V[] toArray(V[] a) {
			if (a.length != 1) {
				a = Arrays.copyOf(a, 1);
			}
			a[0] = (V) entry;
			return a;
		}

		@Override
		public boolean add(T e) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean remove(Object o) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean containsAll(Collection<?> c) {
			int size = c.size();
			if (size != 1) {
				return false;
			}
			for (Object o : c) {
				return contains(o);
			}
			return false;
		}

		@Override
		public boolean addAll(Collection<? extends T> c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean removeAll(Collection<?> c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean retainAll(Collection<?> c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void clear() {
			throw new UnsupportedOperationException();
		}
	}

	static final class SingleEntrySet<T> extends SingleEntryCollection<T> implements Set<T> {

		SingleEntrySet(T entry) {
			super(entry);
		}
	}

	static final class SingleEntryList<T> extends SingleEntryCollection<T> implements List<T> {

		SingleEntryList(T entry) {
			super(entry);
		}

		@Override
		public boolean addAll(int index, Collection<? extends T> c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public T get(int index) {
			if (index == 0) {
				return entry;
			}
			throw new IndexOutOfBoundsException();
		}

		@Override
		public T set(int index, T element) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void add(int index, T element) {
			throw new UnsupportedOperationException();
		}

		@Override
		public T remove(int index) {
			throw new UnsupportedOperationException();
		}

		@Override
		public int indexOf(Object o) {
			return contains(o) ? 0 : -1;
		}

		@Override
		public int lastIndexOf(Object o) {
			return contains(o) ? 0 : -1;
		}

		@Override
		public ListIterator<T> listIterator() {
			return new SingleEntryListIterator<T>(entry);
		}

		@Override
		public ListIterator<T> listIterator(int index) {
			if (index != 0) {
				throw new IndexOutOfBoundsException();
			}
			return listIterator();
		}

		@Override
		public List<T> subList(int fromIndex, int toIndex) {
			if (fromIndex == 0) {
				if (toIndex == 0) {
					return Collections.emptyList();
				} else if (toIndex == 1) {
					return this;
				} else {
					throw new IndexOutOfBoundsException();
				}
			} else if (fromIndex == 1 && toIndex == 1) {
				return Collections.emptyList();
			} else {
				throw new IndexOutOfBoundsException();
			}
		}
	}

	static final class SingleEntryMap<K, V> implements Map<K, V> {

		final K key;
		final V value;

		SingleEntryMap(K key, V value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public int size() {
			return 1;
		}

		@Override
		public boolean isEmpty() {
			return false;
		}

		@Override
		public boolean containsKey(Object key) {
			if (this.key == null) {
				return key == null;
			}
			return this.key.equals(key);
		}

		@Override
		public boolean containsValue(Object value) {
			if (this.value == null) {
				return value == null;
			}
			return this.value.equals(value);
		}

		@Override
		public V get(Object key) {
			if (this.key == null) {
				return key == null ? this.value : null;
			}
			return this.key.equals(key) ? this.value : null;
		}

		@Override
		public V put(K key, V value) {
			throw new UnsupportedOperationException();
		}

		@Override
		public V remove(Object key) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void putAll(Map<? extends K, ? extends V> m) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void clear() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Set<K> keySet() {
			return new SingleEntrySet<K>(this.key);
		}

		@Override
		public Collection<V> values() {
			return new SingleEntrySet<V>(this.value);
		}

		@Override
		public Set<Entry<K, V>> entrySet() {
			return new SingleEntrySet<Entry<K, V>>(new Map.Entry<K, V>() {

				@Override
				public K getKey() {
					return key;
				}

				@Override
				public V getValue() {
					return value;
				}

				@Override
				public V setValue(V value) {
					throw new UnsupportedOperationException();
				}
			});
		}
	}
}
