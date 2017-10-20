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
package net.helenus.core;

import java.util.*;

import com.diffplug.common.base.Errors;
import com.google.common.collect.TreeTraverser;

/** Encapsulates the concept of a "transaction" as a unit-of-work. */
public abstract class AbstractUnitOfWork<E extends Exception> implements UnitOfWork, AutoCloseable {
	private final List<AbstractUnitOfWork<E>> nested = new ArrayList<>();
	private final HelenusSession session;
	private final AbstractUnitOfWork<E> parent;
	private List<CommitThunk> postCommit = new ArrayList<CommitThunk>();
	private final Map<String, Set<Object>> cache = new HashMap<String, Set<Object>>();
	private boolean aborted = false;
	private boolean committed = false;

	protected AbstractUnitOfWork(HelenusSession session, AbstractUnitOfWork<E> parent) {
		Objects.requireNonNull(session, "containing session cannot be null");

		this.session = session;
		this.parent = parent;
	}

	public UnitOfWork addNestedUnitOfWork(UnitOfWork uow) {
		synchronized (nested) {
			nested.add((AbstractUnitOfWork<E>) uow);
		}
		return this;
	}

	public UnitOfWork begin() {
		// log.record(txn::start)
		return this;
	}

	private void applyPostCommitFunctions() {
		if (!postCommit.isEmpty()) {
			for (CommitThunk f : postCommit) {
				f.apply();
			}
		}
	}

	public Set<Object> cacheLookup(String key) {
		Set<Object> r = getCache().get(key);
		if (r != null) {
			return r;
		} else {
			if (parent != null) {
				return parent.cacheLookup(key);
			}
		}
		return null;
	}

	public Map<String, Set<Object>> getCache() {
		return cache;
	}

	private Iterator<AbstractUnitOfWork<E>> getChildNodes() {
		return nested.iterator();
	}

	/**
	 * Checks to see if the work performed between calling begin and now can be
	 * committed or not.
	 *
	 * @return a function from which to chain work that only happens when commit is
	 *         successful
	 * @throws E
	 *             when the work overlaps with other concurrent writers.
	 */
	public PostCommitFunction<Void, Void> commit() throws E {
		// All nested UnitOfWork should be committed (not aborted) before calls to
		// commit, check.
		boolean canCommit = true;
		TreeTraverser<AbstractUnitOfWork<E>> traverser = TreeTraverser.using(node -> node::getChildNodes);
		for (AbstractUnitOfWork<E> uow : traverser.postOrderTraversal(this)) {
			if (this != uow) {
				canCommit &= (!uow.aborted && uow.committed);
			}
		}

		// log.record(txn::provisionalCommit)
		// examine log for conflicts in read-set and write-set between begin and
		// provisional commit
		// if (conflict) { throw new ConflictingUnitOfWorkException(this) }
		// else return function so as to enable commit.andThen(() -> { do something iff
		// commit was successful; })

		if (canCommit) {
			committed = true;
			aborted = false;

			// TODO(gburd): union this cache with parent's (if there is a parent) or with
			// the session cache for all cacheable entities we currently hold

			nested.forEach((uow) -> Errors.rethrow().wrap(uow::commit));

			// Merge UOW cache into parent's cache.
			if (parent != null) {
				Map<String, Set<Object>> parentCache = parent.getCache();
				for (String key : cache.keySet()) {
					if (parentCache.containsKey(key)) {
						// merge the sets
						Set<Object> ps = parentCache.get(key);
						ps.addAll(cache.get(key)); // TODO(gburd): review this, likely not correct in all cases as-is.
					} else {
						// add the missing set
						parentCache.put(key, cache.get(key));
					}
				}
			}

			// Apply all post-commit functions for
			if (parent == null) {
				traverser.postOrderTraversal(this).forEach(uow -> {
					uow.applyPostCommitFunctions();
				});
				return new PostCommitFunction(this, null);
			}
		}
		// else {
		// Constructor<T> ctor = clazz.getConstructor(conflictExceptionClass);
		// T object = ctor.newInstance(new Object[] { String message });
		// }
		return new PostCommitFunction(this, postCommit);
	}

	/* Explicitly discard the work and mark it as as such in the log. */
	public void abort() {
		TreeTraverser<AbstractUnitOfWork<E>> traverser = TreeTraverser.using(node -> node::getChildNodes);
		traverser.postOrderTraversal(this).forEach(uow -> {
			uow.committed = false;
			uow.aborted = true;
		});
		// log.record(txn::abort)
		// cache.invalidateSince(txn::start time)
	}

	public String describeConflicts() {
		return "it's complex...";
	}

	@Override
	public void close() throws E {
		// Closing a AbstractUnitOfWork will abort iff we've not already aborted or
		// committed this unit of work.
		if (aborted == false && committed == false) {
			abort();
		}
	}

	public boolean hasAborted() {
		return aborted;
	}

	public boolean hasCommitted() {
		return committed;
	}
}
