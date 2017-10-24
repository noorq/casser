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

import java.util.List;
import java.util.Optional;

import com.google.common.base.Stopwatch;

import net.helenus.core.cache.Facet;

public interface UnitOfWork<X extends Exception> extends AutoCloseable {

	/**
	 * Marks the beginning of a transactional section of work. Will write a record
	 * to the shared write-ahead log.
	 *
	 * @return the handle used to commit or abort the work.
	 */
	UnitOfWork<X> begin();

	void addNestedUnitOfWork(UnitOfWork<X> uow);

	/**
	 * Checks to see if the work performed between calling begin and now can be
	 * committed or not.
	 *
	 * @return a function from which to chain work that only happens when commit is
	 *         successful
	 * @throws X
	 *             when the work overlaps with other concurrent writers.
	 */
	PostCommitFunction<Void, Void> commit() throws X;

	/**
	 * Explicitly abort the work within this unit of work. Any nested aborted unit
	 * of work will trigger the entire unit of work to commit.
	 */
	void abort();

	boolean hasAborted();

	boolean hasCommitted();

	Optional<Object> cacheLookup(List<Facet> facets);

	void cacheUpdate(Object pojo, List<Facet> facets);

	UnitOfWork setPurpose(String purpose);

	Stopwatch getExecutionTimer();

	Stopwatch getCacheLookupTimer();

	void record(int cache, int ops);

}
