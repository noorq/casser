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
package net.helenus.support;

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
		} else if (right != null) {
			return EitherCase.RIGHT;
		}
		throw new IllegalStateException("unexpected state");
	}

	public <T> T fold(Function<L, T> leftFunction, Function<R, T> rightFunction) {
		switch (getCase()) {
			case LEFT :
				return leftFunction.apply(left);
			case RIGHT :
				return rightFunction.apply(right);
		}
		throw new IllegalStateException("unexpected state");
	}

	@Override
	public String toString() {
		if (left != null) {
			return "[" + left + ",]";
		}
		return "[," + right + "]";
	}
}
