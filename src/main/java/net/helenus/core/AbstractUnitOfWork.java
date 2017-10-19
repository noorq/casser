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

import com.diffplug.common.base.Errors;
import com.google.common.collect.TreeTraverser;
import java.util.*;
import java.util.stream.Collectors;
import net.helenus.core.cache.BoundFacet;
import net.helenus.support.Either;
import org.ahocorasick.trie.Emit;
import org.ahocorasick.trie.Trie;

/** Encapsulates the concept of a "transaction" as a unit-of-work. */
public abstract class AbstractUnitOfWork<E extends Exception>
    implements UnitOfWork<E>, AutoCloseable {
  private final List<AbstractUnitOfWork<E>> nested = new ArrayList<>();
  private final HelenusSession session;
  private final AbstractUnitOfWork<E> parent;
  private List<CommitThunk> postCommit = new ArrayList<CommitThunk>();
  private final Map<String, Either<Object, Set<Object>>> cache =
      new HashMap<String, Either<Object, Set<Object>>>();
  private Trie cacheIndex =
      Trie.builder().ignoreOverlaps().onlyWholeWordsWhiteSpaceSeparated().build();
  private boolean aborted = false;
  private boolean committed = false;

  protected AbstractUnitOfWork(HelenusSession session, AbstractUnitOfWork<E> parent) {
    Objects.requireNonNull(session, "containing session cannot be null");

    this.session = session;
    this.parent = parent;
  }

  @Override
  public void addNestedUnitOfWork(UnitOfWork<E> uow) {
    synchronized (nested) {
      nested.add((AbstractUnitOfWork<E>) uow);
    }
  }

  @Override
  public UnitOfWork<E> begin() {
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

  @Override
  public Optional<Either<Object, Set<Object>>> cacheLookupByFacet(Set<BoundFacet> facets) {
    Optional<Either<Object, Set<Object>>> result = Optional.empty();
    Collection<Emit> emits =
        cacheIndex.parseText(
            String.join(
                " ", facets.stream().map(facet -> facet.toString()).collect(Collectors.toList())));
    for (Emit emit : emits) {
      // NOTE: rethink. should this match *all* facets?  how do I know which emit keyword is the primary key?
      String key = emit.getKeyword();
      result = cacheLookup(key);
      if (result.isPresent()) {
        return result;
      }
    }
    if (!result.isPresent()) {
      // Be sure to check all enclosing UnitOfWork caches as well, we may be nested.
      if (parent != null) {
        return parent.cacheLookupByFacet(facets);
      }
    }
    return result;
  }

  @Override
  public Optional<Either<Object, Set<Object>>> cacheLookupByStatement(String[] statementKeys) {
    String key = String.join(",", statementKeys);
    return cacheLookup(key);
  }

  @Override
  public Optional<Either<Object, Set<Object>>> cacheLookup(String key) {
    Optional<Either<Object, Set<Object>>> result =
        (cache.containsKey(key)) ? Optional.of(cache.get(key)) : Optional.empty();

    if (!result.isPresent()) {
      // Be sure to check all enclosing UnitOfWork caches as well, we may be nested.
      if (parent != null) {
        return parent.cacheLookup(key);
      }
    }
    return result;
  }

  @Override
  public void cacheUpdate(
      Either<Object, Set<Object>> value, String[] statementKeys, Map<String, BoundFacet> facetMap) {
    String key = "CQL::" + String.join(",", statementKeys);
    cache.put(key, value);
    Trie.TrieBuilder builder =
        cacheIndex.builder().ignoreOverlaps().onlyWholeWordsWhiteSpaceSeparated();
    facetMap.forEach(
        (facetName, facet) -> {
          builder.addKeyword(facet.toString());
          if (facetName.equals("*")) {
            cache.put(facet.toString(), value);
          }
        });
    cacheIndex = builder.build();
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
  public PostCommitFunction<Void, Void> commit() throws E {
    // All nested UnitOfWork should be committed (not aborted) before calls to commit, check.
    boolean canCommit = true;
    TreeTraverser<AbstractUnitOfWork<E>> traverser =
        TreeTraverser.using(node -> node::getChildNodes);
    for (AbstractUnitOfWork<E> uow : traverser.postOrderTraversal(this)) {
      if (this != uow) {
        canCommit &= (!uow.aborted && uow.committed);
      }
    }

    // log.record(txn::provisionalCommit)
    // examine log for conflicts in read-set and write-set between begin and provisional commit
    // if (conflict) { throw new ConflictingUnitOfWorkException(this) }
    // else return function so as to enable commit.andThen(() -> { do something iff commit was successful; })

    if (canCommit) {
      committed = true;
      aborted = false;

      // TODO(gburd): union this cache with parent's (if there is a parent) or with the session cache for all cacheable entities we currently hold

      nested.forEach((uow) -> Errors.rethrow().wrap(uow::commit));

      // Merge UOW cache into parent's cache.
      if (parent != null) {
        parent.assumeCache(cache, cacheIndex);
      }

      // Apply all post-commit functions for
      if (parent == null) {
        traverser
            .postOrderTraversal(this)
            .forEach(
                uow -> {
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
    TreeTraverser<AbstractUnitOfWork<E>> traverser =
        TreeTraverser.using(node -> node::getChildNodes);
    traverser
        .postOrderTraversal(this)
        .forEach(
            uow -> {
              uow.committed = false;
              uow.aborted = true;
            });
    // log.record(txn::abort)
    // cache.invalidateSince(txn::start time)
  }

  private void assumeCache(
      Map<String, Either<Object, Set<Object>>> childCache, Trie childCacheIndex) {
    for (String key : childCache.keySet()) {
      if (cache.containsKey(key)) {
        Either<Object, Set<Object>> value = cache.get(key);
        if (value.isLeft()) {
          Object obj = value.getLeft();
          // merge objects
          Either<Object, Set<Object>> childValue = childCache.get(key);
          if (childValue.isLeft()) {
            Object childObj = childValue.getLeft();
          } else {
            Set<Object> childSet = childValue.getRight();
          }
        } else {
          // merge the sets
          Set<Object> set = value.getRight();
          Either<Object, Set<Object>> childValue = childCache.get(key);
          if (childValue.isLeft()) {
            Object childObj = childValue.getLeft();
            set.add(childObj);
          } else {
            Set<Object> childSet = childValue.getRight();
            set.addAll(childSet);
          }
        }
      } else {
        cache.put(key, childCache.get(key));
      }
    }
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
}
