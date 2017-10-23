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
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.diffplug.common.base.Errors;
import com.google.common.base.Stopwatch;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.TreeTraverser;

import net.helenus.core.cache.CacheUtil;
import net.helenus.core.cache.Facet;

/** Encapsulates the concept of a "transaction" as a unit-of-work. */
public abstract class AbstractUnitOfWork<E extends Exception> implements UnitOfWork<E>, AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractUnitOfWork.class);

	private final List<AbstractUnitOfWork<E>> nested = new ArrayList<>();
	private final HelenusSession session;
	private final AbstractUnitOfWork<E> parent;
	// Cache:
	private final Table<String, String, Object> cache = HashBasedTable.create();
	private List<CommitThunk> postCommit = new ArrayList<CommitThunk>();
	private boolean aborted = false;
	private boolean committed = false;
	private String purpose_;
	private Stopwatch elapsedTime_;
	private Stopwatch databaseTime_ = Stopwatch.createUnstarted();
	private Stopwatch cacheLookupTime_ = Stopwatch.createUnstarted();

	protected AbstractUnitOfWork(HelenusSession session, AbstractUnitOfWork<E> parent) {
		Objects.requireNonNull(session, "containing session cannot be null");

		this.session = session;
		this.parent = parent;
	}

	@Override
	public Stopwatch getExecutionTimer() {
		return databaseTime_;
	}

	@Override
	public Stopwatch getCacheLookupTimer() {
		return cacheLookupTime_;
	}

	@Override
	public void addNestedUnitOfWork(UnitOfWork<E> uow) {
		synchronized (nested) {
			nested.add((AbstractUnitOfWork<E>) uow);
		}
	}

	@Override
	public UnitOfWork<E> begin() {
		elapsedTime_ = Stopwatch.createStarted();
		// log.record(txn::start)
		return this;
	}

	@Override
	public UnitOfWork setPurpose(String purpose) {
		purpose_ = purpose;
		return this;
	}

	public void logTimers(String what) {

	    cache hit, miss;
        multiple calls, sometimes to db, sometimes to cache, sometimes both...
        uow.setPurpose(getClass().getSimpleName() + "::" + Thread.currentThread().getStackTrace()[1].getMethodName());


		double e = (double) elapsedTime_.elapsed(TimeUnit.MICROSECONDS) / 1000.0;
		double d = (double) databaseTime_.elapsed(TimeUnit.MICROSECONDS) / 1000.0;
		double c = (double) cacheLookupTime_.elapsed(TimeUnit.MICROSECONDS) / 1000.0;
		double fd = (d / (e - c)) * 100.0;
		double fc = (c / (e - d)) * 100.0;
		LOG.info(String.format("UOW(%s)%s %s (total: %.3fms cache: %.3fms %2.2f%% db: %.3fms %2.2f%%)", hashCode(),
				(purpose_ == null ? "" : " " + purpose_), what, e, c, fc, d, fd));
	}

	private void applyPostCommitFunctions() {
		if (!postCommit.isEmpty()) {
			for (CommitThunk f : postCommit) {
				f.apply();
			}
		}
		logTimers("committed");
	}

	@Override
	public Optional<Object> cacheLookup(List<Facet> facets) {
		String tableName = CacheUtil.schemaName(facets);
		Optional<Object> result = Optional.empty();
		for (Facet facet : facets) {
			if (!facet.fixed()) {
				String columnName = facet.name() + "==" + facet.value();
				Object value = cache.get(tableName, columnName);
				if (value != null) {
					if (result.isPresent() && result.get() != value) {
						// One facet matched, but another did not.
						result = Optional.empty();
						break;
					} else {
						result = Optional.of(value);
					}
				}
			}
		}
		if (!result.isPresent()) {
			// Be sure to check all enclosing UnitOfWork caches as well, we may be nested.
			if (parent != null) {
				return parent.cacheLookup(facets);
			}
		}
		return result;
	}

	@Override
	public void cacheUpdate(Object value, List<Facet> facets) {
		Facet table = facets.remove(0);
		String tableName = table.value().toString();
		for (Facet facet : facets) {
			String columnName = facet.name() + "==" + facet.value();
			cache.put(tableName, columnName, value);
		}
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

			nested.forEach((uow) -> Errors.rethrow().wrap(uow::commit));

			// Merge UOW cache into parent's cache.
			if (parent != null) {
				parent.mergeCache(cache);
			} else {
				session.mergeCache(cache);
			}
			elapsedTime_.stop();

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
		if (!hasAborted()) {
			elapsedTime_.stop();
			logTimers("aborted");
		}
	}

	private void mergeCache(Table<String, String, Object> from) {
		Table<String, String, Object> to = this.cache;
		from.rowMap().forEach((rowKey, columnMap) -> {
			columnMap.forEach((columnKey, value) -> {
				if (to.contains(rowKey, columnKey)) {
					to.put(rowKey, columnKey, CacheUtil.merge(to.get(rowKey, columnKey), from.get(rowKey, columnKey)));
				} else {
					to.put(rowKey, columnKey, from.get(rowKey, columnKey));
				}
			});
		});
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
