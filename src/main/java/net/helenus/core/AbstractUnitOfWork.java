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

import static net.helenus.core.HelenusSession.deleted;

import com.google.common.base.Stopwatch;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.TreeTraverser;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import net.helenus.core.cache.CacheUtil;
import net.helenus.core.cache.Facet;
import net.helenus.core.operation.AbstractOperation;
import net.helenus.core.operation.BatchOperation;
import net.helenus.mapping.MappingUtil;
import net.helenus.support.Either;
import net.helenus.support.HelenusException;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Encapsulates the concept of a "transaction" as a unit-of-work. */
public abstract class AbstractUnitOfWork<E extends Exception>
    implements UnitOfWork<E>, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractUnitOfWork.class);

  private final List<AbstractUnitOfWork<E>> nested = new ArrayList<>();
  private final HelenusSession session;
  private final AbstractUnitOfWork<E> parent;
  private final Table<String, String, Either<Object, List<Facet>>> cache = HashBasedTable.create();
  private final Map<String, Object> statementCache = new ConcurrentHashMap<String, Object>();
  protected String purpose;
  protected List<String> nestedPurposes = new ArrayList<String>();
  protected String info;
  protected int cacheHits = 0;
  protected int cacheMisses = 0;
  protected int databaseLookups = 0;
  protected Stopwatch elapsedTime;
  protected Map<String, Double> databaseTime = new HashMap<>();
  protected double cacheLookupTimeMSecs = 0.0;
  private List<CommitThunk> commitThunks = new ArrayList<CommitThunk>();
  private List<CommitThunk> abortThunks = new ArrayList<CommitThunk>();
  private List<CompletableFuture<?>> asyncOperationFutures = new ArrayList<CompletableFuture<?>>();
  private boolean aborted = false;
  private boolean committed = false;
  private long committedAt = 0L;
  private BatchOperation batch;

  protected AbstractUnitOfWork(HelenusSession session, AbstractUnitOfWork<E> parent) {
    Objects.requireNonNull(session, "containing session cannot be null");

    this.session = session;
    this.parent = parent;
  }

  @Override
  public void addDatabaseTime(String name, Stopwatch amount) {
    Double time = databaseTime.get(name);
    if (time == null) {
      databaseTime.put(name, (double) amount.elapsed(TimeUnit.MICROSECONDS));
    } else {
      databaseTime.put(name, time + amount.elapsed(TimeUnit.MICROSECONDS));
    }
  }

  @Override
  public void addCacheLookupTime(Stopwatch amount) {
    cacheLookupTimeMSecs += amount.elapsed(TimeUnit.MICROSECONDS);
  }

  @Override
  public void addNestedUnitOfWork(UnitOfWork<E> uow) {
    synchronized (nested) {
      nested.add((AbstractUnitOfWork<E>) uow);
    }
  }

  @Override
  public synchronized UnitOfWork<E> begin() {
    elapsedTime = Stopwatch.createStarted();
    // log.record(txn::start)
    return this;
  }

  @Override
  public String getPurpose() {
    return purpose;
  }

  @Override
  public UnitOfWork setPurpose(String purpose) {
    this.purpose = purpose;
    return this;
  }

  @Override
  public void addFuture(CompletableFuture<?> future) {
    asyncOperationFutures.add(future);
  }

  @Override
  public void setInfo(String info) {
    this.info = info;
  }

  @Override
  public void recordCacheAndDatabaseOperationCount(int cache, int ops) {
    if (cache > 0) {
      cacheHits += cache;
    } else {
      cacheMisses += Math.abs(cache);
    }
    if (ops > 0) {
      databaseLookups += ops;
    }
  }

  public String logTimers(String what) {
    double e = (double) elapsedTime.elapsed(TimeUnit.MICROSECONDS) / 1000.0;
    double d = 0.0;
    double c = cacheLookupTimeMSecs / 1000.0;
    double fc = (c / e) * 100.0;
    String database = "";
    if (databaseTime.size() > 0) {
      List<String> dbt = new ArrayList<>(databaseTime.size());
      for (Map.Entry<String, Double> dt : databaseTime.entrySet()) {
        double t = dt.getValue() / 1000.0;
        d += t;
        dbt.add(String.format("%s took %,.3fms %,2.2f%%", dt.getKey(), t, (t / e) * 100.0));
      }
      double fd = (d / e) * 100.0;
      database =
          String.format(
              ", %d quer%s (%,.3fms %,2.2f%% - %s)",
              databaseLookups, (databaseLookups > 1) ? "ies" : "y", d, fd, String.join(", ", dbt));
    }
    String cache = "";
    if (cacheLookupTimeMSecs > 0) {
      int cacheLookups = cacheHits + cacheMisses;
      cache =
          String.format(
              " with %d cache lookup%s (%,.3fms %,2.2f%% - %,d hit, %,d miss)",
              cacheLookups, cacheLookups > 1 ? "s" : "", c, fc, cacheHits, cacheMisses);
    }
    String da = "";
    if (databaseTime.size() > 0 || cacheLookupTimeMSecs > 0) {
      double dat = d + c;
      double daf = (dat / e) * 100;
      da =
          String.format(
              " consuming %,.3fms for data access, or %,2.2f%% of total UOW time.", dat, daf);
    }
    String x = nestedPurposes.stream().distinct().collect(Collectors.joining(", "));
    String n =
        nested
            .stream()
            .map(uow -> String.valueOf(uow.hashCode()))
            .collect(Collectors.joining(", "));
    String s =
        String.format(
            Locale.US,
            "UOW(%s%s) %s in %,.3fms%s%s%s%s%s%s",
            hashCode(),
            (nested.size() > 0 ? ", [" + n + "]" : ""),
            what,
            e,
            cache,
            database,
            da,
            (purpose == null ? "" : " " + purpose),
            (nestedPurposes.isEmpty()) ? "" : ", " + x,
            (info == null) ? "" : " " + info);
    return s;
  }

  private void applyPostCommitFunctions(String what, List<CommitThunk> thunks) {
    if (!thunks.isEmpty()) {
      for (CommitThunk f : thunks) {
        f.apply();
      }
    }
    if (LOG.isInfoEnabled()) {
      LOG.info(logTimers(what));
    }
  }

  @Override
  public Optional<Object> cacheLookup(String key) {
      AbstractUnitOfWork self = this;
      do {
          Object result = self.statementCache.get(key);
          if (result != null) {
              return result == deleted ? Optional.ofNullable(null) : Optional.of(result);
          }
          self = self.parent;
      } while (self != null);
      return Optional.empty();
  }

  @Override
  public Optional<Object> cacheLookup(List<Facet> facets) {
    String tableName = CacheUtil.schemaName(facets);
      Optional<Object> result = Optional.empty();
    for (Facet facet : facets) {
      if (!facet.fixed()) {
        String columnName = facet.name() + "==" + facet.value();
        Either<Object, List<Facet>> eitherValue = cache.get(tableName, columnName);
        if (eitherValue != null) {
          Object value = deleted;
          if (eitherValue.isLeft()) {
            value = eitherValue.getLeft();
          }
          return Optional.of(value);
        }
      }
    }

    // Be sure to check all enclosing UnitOfWork caches as well, we may be nested.
    result = checkParentCache(facets);
    if (result.isPresent()) {
      Object r = result.get();
      Class<?> iface = MappingUtil.getMappingInterface(r);
      if (Helenus.entity(iface).isDraftable()) {
        cacheUpdate(r, facets);
      } else {
        cacheUpdate(SerializationUtils.<Serializable>clone((Serializable) r), facets);
      }
    }
    return result;
  }

  private Optional<Object> checkParentCache(List<Facet> facets) {
    Optional<Object> result = Optional.empty();
    if (parent != null) {
      result = parent.checkParentCache(facets);
    }
    return result;
  }

  @Override
  public void cacheEvict(String key) {
      statementCache.remove(key);
  }

  @Override
  public void cacheDelete(String key) {
      statementCache.replace(key, deleted);
  }

  @Override
  public List<Facet> cacheEvict(List<Facet> facets) {
    Either<Object, List<Facet>> deletedObjectFacets = Either.right(facets);
    String tableName = CacheUtil.schemaName(facets);
    Optional<Object> optionalValue = cacheLookup(facets);

    for (Facet facet : facets) {
      if (!facet.fixed()) {
        String columnKey = facet.name() + "==" + facet.value();
        // mark the value identified by the facet to `deleted`
        cache.put(tableName, columnKey, deletedObjectFacets);
      }
    }

    // Now, look for other row/col pairs that referenced the same object, mark them
    // `deleted` if the cache had a value before we added the deleted marker objects.
    if (optionalValue.isPresent()) {
      Object value = optionalValue.get();
      cache
          .columnKeySet()
          .forEach(
              columnKey -> {
                Either<Object, List<Facet>> eitherCachedValue = cache.get(tableName, columnKey);
                if (eitherCachedValue.isLeft()) {
                  Object cachedValue = eitherCachedValue.getLeft();
                  if (cachedValue == value) {
                    cache.put(tableName, columnKey, deletedObjectFacets);
                    String[] parts = columnKey.split("==");
                    facets.add(new Facet<String>(parts[0], parts[1]));
                  }
                }
              });
    }
    return facets;
  }

  @Override
  public Object cacheUpdate(String key, Object value) {
      return statementCache.replace(key, value);
  }

  @Override
  public Object cacheUpdate(Object value, List<Facet> facets) {
    Object result = null;
    String tableName = CacheUtil.schemaName(facets);
    for (Facet facet : facets) {
      if (!facet.fixed()) {
        if (facet.alone()) {
          String columnName = facet.name() + "==" + facet.value();
          if (result == null) result = cache.get(tableName, columnName);
          cache.put(tableName, columnName, Either.left(value));
        }
      }
    }
    return result;
  }

  public void batch(AbstractOperation s) {
    if (batch == null) {
      batch = new BatchOperation(session);
    }
    batch.add(s);
  }

  private Iterator<AbstractUnitOfWork<E>> getChildNodes() {
    return nested.iterator();
  }

  /**
   * Checks to see if the work performed between calling begin and now can be committed or not.
   *
   * @return a function from which to chain work that only happens when commit is successful
   * @throws E when the work overlaps with other concurrent writers.
   */
  public synchronized PostCommitFunction<Void, Void> commit() throws E, TimeoutException {

    if (isDone()) {
      return new PostCommitFunction(this, null, null, false);
    }

    // Only the outer-most UOW batches statements for commit time, execute them.
    if (batch != null) {
      committedAt = batch.sync(this); //TODO(gburd): update cache with writeTime...
    }

    // All nested UnitOfWork should be committed (not aborted) before calls to
    // commit, check.
    boolean canCommit = true;
    TreeTraverser<AbstractUnitOfWork<E>> traverser =
        TreeTraverser.using(node -> node::getChildNodes);
    for (AbstractUnitOfWork<E> uow : traverser.postOrderTraversal(this)) {
      if (this != uow) {
        canCommit &= (!uow.aborted && uow.committed);
      }
    }

    if (!canCommit) {
      elapsedTime.stop();

      if (parent == null) {

        // Apply all post-commit abort functions, this is the outer-most UnitOfWork.
        traverser
            .postOrderTraversal(this)
            .forEach(
                uow -> {
                  applyPostCommitFunctions("aborted", abortThunks);
                });
      }

      return new PostCommitFunction(this, null, null, false);
    } else {
      elapsedTime.stop();
      committed = true;
      aborted = false;

      if (parent == null) {

        // Apply all post-commit commit functions, this is the outer-most UnitOfWork.
        traverser
            .postOrderTraversal(this)
            .forEach(
                uow -> {
                  applyPostCommitFunctions("committed", uow.commitThunks);
                });

        // Merge our cache into the session cache.
        session.mergeCache(cache);

        // Spoil any lingering futures that may be out there.
        asyncOperationFutures.forEach(
            f ->
                f.completeExceptionally(
                    new HelenusException(
                        "Futures must be resolved before their unit of work has committed/aborted.")));

        return new PostCommitFunction(this, null, null, true);
      } else {

        // Merge cache and statistics into parent if there is one.
        parent.statementCache.putAll(statementCache);
        parent.mergeCache(cache);
        parent.addBatched(batch);
        if (purpose != null) {
          parent.nestedPurposes.add(purpose);
        }
        parent.cacheHits += cacheHits;
        parent.cacheMisses += cacheMisses;
        parent.databaseLookups += databaseLookups;
        parent.cacheLookupTimeMSecs += cacheLookupTimeMSecs;
        for (Map.Entry<String, Double> dt : databaseTime.entrySet()) {
          String name = dt.getKey();
          if (parent.databaseTime.containsKey(name)) {
            double t = parent.databaseTime.get(name);
            parent.databaseTime.put(name, t + dt.getValue());
          } else {
            parent.databaseTime.put(name, dt.getValue());
          }
        }
      }
    }
    // TODO(gburd): hopefully we'll be able to detect conflicts here and so we'd want to...
    // else {
    // Constructor<T> ctor = clazz.getConstructor(conflictExceptionClass);
    // T object = ctor.newInstance(new Object[] { String message });
    // }
    return new PostCommitFunction(this, commitThunks, abortThunks, true);
  }

  private void addBatched(BatchOperation batch) {
    if (this.batch == null) {
      this.batch = batch;
    } else {
      this.batch.addAll(batch);
    }
  }

  /* Explicitly discard the work and mark it as as such in the log. */
  public synchronized void abort() {
    if (!aborted) {
      aborted = true;

      // Spoil any pending futures created within the context of this unit of work.
      asyncOperationFutures.forEach(
          f ->
              f.completeExceptionally(
                  new HelenusException(
                      "Futures must be resolved before their unit of work has committed/aborted.")));

      TreeTraverser<AbstractUnitOfWork<E>> traverser =
          TreeTraverser.using(node -> node::getChildNodes);
      traverser
          .postOrderTraversal(this)
          .forEach(
              uow -> {
                applyPostCommitFunctions("aborted", uow.abortThunks);
                uow.abortThunks.clear();
              });

      // TODO(gburd): when we integrate the transaction support we'll need to...
      // log.record(txn::abort)
      // cache.invalidateSince(txn::start time)
    }
  }

  private void mergeCache(Table<String, String, Either<Object, List<Facet>>> from) {
    Table<String, String, Either<Object, List<Facet>>> to = this.cache;
    from.rowMap()
        .forEach(
            (rowKey, columnMap) -> {
              columnMap.forEach(
                  (columnKey, value) -> {
                    if (to.contains(rowKey, columnKey)) {
                      to.put(
                          rowKey,
                          columnKey,
                          Either.left(
                              CacheUtil.merge(
                                  to.get(rowKey, columnKey).getLeft(),
                                  from.get(rowKey, columnKey).getLeft())));
                    } else {
                      to.put(rowKey, columnKey, from.get(rowKey, columnKey));
                    }
                  });
            });
  }

  public boolean isDone() {
      return aborted || committed;
  }

  public String describeConflicts() {
    return "it's complex...";
  }

  @Override
  public void close() throws E {
    // Closing a AbstractUnitOfWork will abort iff we've not already aborted or committed this unit of work.
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

  public long committedAt() {
    return committedAt;
  }
}
