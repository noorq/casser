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

import com.datastax.driver.core.Statement;
import com.google.common.base.Stopwatch;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import net.helenus.core.cache.Facet;
import net.helenus.core.operation.AbstractOperation;

public interface UnitOfWork<X extends Exception> extends AutoCloseable {

  /**
   * Marks the beginning of a transactional section of work. Will write a
   * recordCacheAndDatabaseOperationCount to the shared write-ahead log.
   *
   * @return the handle used to commit or abort the work.
   */
  UnitOfWork<X> begin();

  void addNestedUnitOfWork(UnitOfWork<X> uow);

  /**
   * Checks to see if the work performed between calling begin and now can be committed or not.
   *
   * @return a function from which to chain work that only happens when commit is successful
   * @throws X when the work overlaps with other concurrent writers.
   */
  PostCommitFunction<Void, Void> commit() throws X, TimeoutException;

  /**
   * Explicitly abort the work within this unit of work. Any nested aborted unit of work will
   * trigger the entire unit of work to commit.
   */
  void abort();

  boolean hasAborted();

  boolean hasCommitted();

  long committedAt();

  void batch(AbstractOperation operation);

  Optional<Object> cacheLookup(List<Facet> facets);

  Object cacheUpdate(Object pojo, List<Facet> facets);

  List<Facet> cacheEvict(List<Facet> facets);

  String getPurpose();

  UnitOfWork setPurpose(String purpose);

  void setInfo(String info);

  void addDatabaseTime(String name, Stopwatch amount);

  void addCacheLookupTime(Stopwatch amount);

  // Cache > 0 means "cache hit", < 0 means cache miss.
  void recordCacheAndDatabaseOperationCount(int cache, int database);
}
