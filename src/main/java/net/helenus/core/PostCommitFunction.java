package net.helenus.core;

import java.util.List;
import java.util.Objects;

public class PostCommitFunction<T, R> implements java.util.function.Function<T, R> {

	private final UnitOfWork uow;
	private final List<CommitThunk> postCommit;

	PostCommitFunction(UnitOfWork uow, List<CommitThunk> postCommit) {
		this.uow = uow;
		this.postCommit = postCommit;
	}

	public void andThen(CommitThunk after) {
		Objects.requireNonNull(after);
		if (postCommit == null) {
			after.apply();
		} else {
			postCommit.add(after);
		}
	}

	@Override
	public R apply(T t) {
		return null;
	}
}
