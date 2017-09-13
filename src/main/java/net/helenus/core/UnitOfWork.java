package net.helenus.core;

import com.diffplug.common.base.Errors;
import com.google.common.collect.TreeTraverser;
import net.helenus.support.HelenusException;

import java.util.*;


/** Encapsulates the concept of a "transaction" as a unit-of-work. */
public class UnitOfWork implements AutoCloseable {
  private final List<UnitOfWork> nested = new ArrayList<>();
  private final HelenusSession session;
  private final UnitOfWork parent;
  private List<CommitThunk> postCommit = new ArrayList<CommitThunk>();
  private final Map<String, Set<Object>> cache = new HashMap<String, Set<Object>>();
  private boolean aborted = false;
  private boolean committed = false;

  protected UnitOfWork(HelenusSession session, UnitOfWork parent) {
    Objects.requireNonNull(session, "containing session cannot be null");

    this.session = session;
    this.parent = parent;
  }

  /**
   * Marks the beginning of a transactional section of work. Will write a record to the shared
   * write-ahead log.
   *
   * @return the handle used to commit or abort the work.
   */
  protected UnitOfWork begin() {
    // log.record(txn::start)
    return this;
  }

  protected UnitOfWork addNestedUnitOfWork(UnitOfWork uow) {
    synchronized (nested) {
      nested.add(uow);
    }
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
    UnitOfWork p = this;
    do {
      Set<Object> r = p.getCache().get(key);
      if (r != null) {
        return r;
      }
      p = parent;
    } while(p != null);
    return null;
  }

  public Map<String, Set<Object>> getCache() { return cache; }

  private Iterator<UnitOfWork> getChildNodes() {
    return nested.iterator();
  }

  /**
   * Checks to see if the work performed between calling begin and now can be committed or not.
   *
   * @return a function from which to chain work that only happens when commit is successful
   * @throws ConflictingUnitOfWorkException when the work overlaps with other concurrent writers.
   */
  public PostCommitFunction<Void, Void> commit() throws ConflictingUnitOfWorkException {
    // All nested UnitOfWork should be committed (not aborted) before calls to commit, check.
    boolean canCommit = true;
    TreeTraverser<UnitOfWork> traverser = TreeTraverser.using(node -> node::getChildNodes);
    for (UnitOfWork uow : traverser.postOrderTraversal(this)) {
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
        Map<String, Set<Object>> parentCache = parent.getCache();
        for (String key : cache.keySet()) {
          if (parentCache.containsKey(key)) {
            // merge the sets
            Set<Object> ps = parentCache.get(key);
            ps.addAll(cache.get(key)); //TODO(gburd): review this, likely not correct in all cases as-is.
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
    return new PostCommitFunction(this, postCommit);
  }

  public void rollback() {
    abort();
  }

  /* Explicitly discard the work and mark it as as such in the log. */
  public void abort() {
    TreeTraverser<UnitOfWork> traverser = TreeTraverser.using(node -> node::getChildNodes);
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
  public void close() throws HelenusException {
    // Closing a UnitOfWork will abort iff we've not already aborted or committed this unit of work.
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
