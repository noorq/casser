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

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import net.helenus.support.HelenusMappingException;

public final class SetDsl<V> implements Set<V> {

	private final HelenusPropertyNode parent;

	public SetDsl(HelenusPropertyNode parent) {
		this.parent = parent;
	}

	public HelenusPropertyNode getParent() {
		return parent;
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
	public boolean retainAll(Collection<?> c) {
		throwShouldNeverCall();
		return false;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throwShouldNeverCall();
		return false;
	}

	@Override
	public void clear() {
		throwShouldNeverCall();
	}

	private void throwShouldNeverCall() {
		throw new HelenusMappingException("should be never called");
	}

	@Override
	public String toString() {
		return "SetDsl";
	}
}
