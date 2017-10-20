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

import java.util.Arrays;

public final class Fun {

	private Fun() {
	}

	public static final class ArrayTuple {

		public final Object[] _a;

		public ArrayTuple(Object[] a) {
			this._a = a;
		}

		public static ArrayTuple of(Object[] a) {
			return new ArrayTuple(a);
		}

		@Override
		public String toString() {
			return "ArrayTuple " + Arrays.toString(_a);
		}
	}

	public static final class Tuple1<A> {

		public final A _1;

		public Tuple1(A v1) {
			this._1 = v1;
		}

		public static <A> Tuple1<A> of(A _1) {
			return new Tuple1<A>(_1);
		}

		@Override
		public String toString() {
			return "Tuple1 [_1=" + _1 + "]";
		}
	}

	public static final class Tuple2<A, B> {

		public final A _1;
		public final B _2;

		public Tuple2(A v1, B v2) {
			this._1 = v1;
			this._2 = v2;
		}

		public static <A, B> Tuple2<A, B> of(A _1, B _2) {
			return new Tuple2<A, B>(_1, _2);
		}

		@Override
		public String toString() {
			return "Tuple2 [_1=" + _1 + ", _2=" + _2 + "]";
		}
	}

	public static final class Tuple3<A, B, C> {

		public final A _1;
		public final B _2;
		public final C _3;

		public Tuple3(A v1, B v2, C v3) {
			this._1 = v1;
			this._2 = v2;
			this._3 = v3;
		}

		public static <A, B, C> Tuple3<A, B, C> of(A _1, B _2, C _3) {
			return new Tuple3<A, B, C>(_1, _2, _3);
		}

		@Override
		public String toString() {
			return "Tuple3 [_1=" + _1 + ", _2=" + _2 + ", _3=" + _3 + "]";
		}
	}

	public static final class Tuple4<A, B, C, D> {

		public final A _1;
		public final B _2;
		public final C _3;
		public final D _4;

		public Tuple4(A v1, B v2, C v3, D v4) {
			this._1 = v1;
			this._2 = v2;
			this._3 = v3;
			this._4 = v4;
		}

		public static <A, B, C, D> Tuple4<A, B, C, D> of(A _1, B _2, C _3, D _4) {
			return new Tuple4<A, B, C, D>(_1, _2, _3, _4);
		}

		@Override
		public String toString() {
			return "Tuple4 [_1=" + _1 + ", _2=" + _2 + ", _3=" + _3 + ", _4=" + _4 + "]";
		}
	}

	public static final class Tuple5<A, B, C, D, E> {

		public final A _1;
		public final B _2;
		public final C _3;
		public final D _4;
		public final E _5;

		public Tuple5(A v1, B v2, C v3, D v4, E v5) {
			this._1 = v1;
			this._2 = v2;
			this._3 = v3;
			this._4 = v4;
			this._5 = v5;
		}

		public static <A, B, C, D, E> Tuple5<A, B, C, D, E> of(A _1, B _2, C _3, D _4, E _5) {
			return new Tuple5<A, B, C, D, E>(_1, _2, _3, _4, _5);
		}

		@Override
		public String toString() {
			return "Tuple5 [_1=" + _1 + ", _2=" + _2 + ", _3=" + _3 + ", _4=" + _4 + ", _5=" + _5 + "]";
		}
	}

	public static final class Tuple6<A, B, C, D, E, F> {

		public final A _1;
		public final B _2;
		public final C _3;
		public final D _4;
		public final E _5;
		public final F _6;

		public Tuple6(A v1, B v2, C v3, D v4, E v5, F v6) {
			this._1 = v1;
			this._2 = v2;
			this._3 = v3;
			this._4 = v4;
			this._5 = v5;
			this._6 = v6;
		}

		public static <A, B, C, D, E, F> Tuple6<A, B, C, D, E, F> of(A _1, B _2, C _3, D _4, E _5, F _6) {
			return new Tuple6<A, B, C, D, E, F>(_1, _2, _3, _4, _5, _6);
		}

		@Override
		public String toString() {
			return "Tuple6 [_1=" + _1 + ", _2=" + _2 + ", _3=" + _3 + ", _4=" + _4 + ", _5=" + _5 + ", _6=" + _6 + "]";
		}
	}

	public static final class Tuple7<A, B, C, D, E, F, G> {

		public final A _1;
		public final B _2;
		public final C _3;
		public final D _4;
		public final E _5;
		public final F _6;
		public final G _7;

		public Tuple7(A v1, B v2, C v3, D v4, E v5, F v6, G v7) {
			this._1 = v1;
			this._2 = v2;
			this._3 = v3;
			this._4 = v4;
			this._5 = v5;
			this._6 = v6;
			this._7 = v7;
		}

		public static <A, B, C, D, E, F, G> Tuple7<A, B, C, D, E, F, G> of(A _1, B _2, C _3, D _4, E _5, F _6, G _7) {
			return new Tuple7<A, B, C, D, E, F, G>(_1, _2, _3, _4, _5, _6, _7);
		}

		@Override
		public String toString() {
			return "Tuple7 [_1=" + _1 + ", _2=" + _2 + ", _3=" + _3 + ", _4=" + _4 + ", _5=" + _5 + ", _6=" + _6
					+ ", _7=" + _7 + "]";
		}
	}
}
