package net.helenus.core;

import com.diffplug.common.base.Errors;
import com.google.common.collect.TreeTraverser;

import java.util.*;
import java.util.function.Function;

/** Encapsulates the concept of a "transaction" as a unit-of-work. */
public class UnitOfWork {

  static private final Map<HelenusSession, UnitOfWork> all = new HashMap<HelenusSession, UnitOfWork>();
  static private final List<UnitOfWork> nested = new ArrayList<>();

  private final HelenusSession session;
  private ArrayList<Function> postCommit = new ArrayList<Function>();
  private boolean aborted = false;
  private boolean committed = false;

  /**
   * Marks the beginning of a transactional section of work. Will write a record to the shared
   * write-ahead log.
   *
   * @return the handle used to commit or abort the work.
   */
  static UnitOfWork begin(HelenusSession session) {
    Objects.requireNonNull(session, "containing session cannot be null");
    UnitOfWork uow = new UnitOfWork(session);
    synchronized (all) {
      all.put(session, uow);
    }
    return uow;
  }

  /**
   * Marks the beginning of a transactional section of work. Will write a record to the shared
   * write-ahead log.
   *
   * @return the handle used to commit or abort the work.
   */
  static UnitOfWork begin(UnitOfWork parent) {
    Objects.requireNonNull(parent, "parent unit of work cannot be null");
    Objects.requireNonNull(all.get(parent), "parent unit of work is not currently active");

    UnitOfWork uow = new UnitOfWork(parent.session);
    synchronized (all) {
      all.put(parent.session, uow);
      parent.addNestedUnitOfWork(uow);
    }
    return uow;
  }

  private UnitOfWork(HelenusSession session) {
    this.session = session;
    // log.record(txn::start)
  }

  private void addNestedUnitOfWork(UnitOfWork uow) {
    synchronized (nested) {
      nested.add(uow);
    }
  }

  private void applyPostCommitFunctions() {
    for(Function f : postCommit) {
      f.apply(null);
    }
  }

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
    for (UnitOfWork uow : traverser.postOrderTraversal(this)) { canCommit &= (!uow.aborted && uow.committed); }

    traverser.postOrderTraversal(this).forEach(uow -> { uow.applyPostCommitFunctions(); });

    nested.forEach((uow) -> Errors.rethrow().wrap(uow::commit));
    // log.record(txn::provisionalCommit)
    // examine log for conflicts in read-set and write-set between begin and provisional commit
    // if (conflict) { throw new ConflictingUnitOfWorkException(this) }
    // else return function so as to enable commit.andThen(() -> { do something iff commit was successful; })

    return new PostCommitFunction<Void, Void>(this);
  }


  public void rollback() {
    abort();
  }

  /** Explicitly discard the work and mark it as as such in the log. */
  public void abort() {
    // log.record(txn::abort)
    // cache.invalidateSince(txn::start time)
    TreeTraverser<UnitOfWork> traverser = TreeTraverser.using(node -> node::getChildNodes);
    traverser.postOrderTraversal(this).forEach(uow -> { uow.aborted = true; });
  }

  public String describeConflicts() {
    return "it's complex...";
  }

  private class PostCommitFunction<T, R> implements java.util.function.Function<T, R> {

    private final UnitOfWork uow;

    PostCommitFunction(UnitOfWork uow) {
      this.uow = uow;
    }

    @Override
    public <V> PostCommitFunction<T, V> andThen(Function<? super R, ? extends V> after) {
      Objects.requireNonNull(after);
      postCommit.add(after);
      return null;
    }

    @Override
    public R apply(T t) {
      return null;
    }
  }
}
