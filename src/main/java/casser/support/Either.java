package casser.support;

import java.util.function.Function;

public final class Either<L, R> {

	private final L left;
	private final R right;
	
	private Either(L left, R right) {
		this.left = left;
		this.right = right;
	}
	
	public boolean isLeft() {
		return left != null;
	}
	
	public L getLeft() {
		return left;
	}

	public boolean isRight() {
		return right != null;
	}

	public R getRight() {
		return right;
	}

	public static <L, R> Either<L, R> left(L left) {
		return new Either<L, R>(left, null);
	}

	public static <L, R> Either<L, R> right(R right) {
		return new Either<L, R>(null, right);
	}

	public EitherCase getCase() {
		if (left != null) {
			return EitherCase.LEFT;
		}
		else if (right != null) {
			return EitherCase.RIGHT;
		}
		throw new IllegalStateException("unexpected state");
	}
	
	public <T> T fold(Function<L, T> leftFunction, Function<R,T> rightFunction) {
		switch(getCase()) {
		case LEFT:
			return leftFunction.apply(left);
		case RIGHT:
			return rightFunction.apply(right);
		}
		throw new IllegalStateException("unexpected state");
	}
	
}
