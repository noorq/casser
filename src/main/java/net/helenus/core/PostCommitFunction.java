package net.helenus.core;

import java.util.List;
import java.util.Objects;

public class PostCommitFunction<T, R> implements java.util.function.Function<T, R> {

  private final UnitOfWork uow;
  private final List<CommitThunk> commitThunks;
  private final List<CommitThunk> abortThunks;
  private boolean committed;

  PostCommitFunction(
      UnitOfWork uow,
      List<CommitThunk> postCommit,
      List<CommitThunk> abortThunks,
      boolean committed) {
    this.uow = uow;
    this.commitThunks = postCommit;
    this.abortThunks = abortThunks;
    this.committed = committed;
  }

  public PostCommitFunction<T, R> andThen(CommitThunk after) {
    Objects.requireNonNull(after);
    if (commitThunks == null) {
      if (committed) {
        after.apply();
      }
    } else {
      commitThunks.add(after);
    }
    return this;
  }

  public PostCommitFunction<T, R> exceptionally(CommitThunk after) {
    Objects.requireNonNull(after);
    if (abortThunks == null) {
      if (!committed) {
        after.apply();
      }
    } else {
      abortThunks.add(after);
    }
    return this;
  }

  @Override
  public R apply(T t) {
    return null;
  }
}
