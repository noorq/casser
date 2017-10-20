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

import java.util.function.Function;

import com.datastax.driver.core.Row;

import net.helenus.core.reflect.HelenusPropertyNode;
import net.helenus.mapping.HelenusProperty;
import net.helenus.mapping.value.ColumnValueProvider;
import net.helenus.support.Fun;

public final class Mappers {

	private Mappers() {
	}

	public static final class Mapper1<A> implements Function<Row, Fun.Tuple1<A>> {

		private final ColumnValueProvider provider;
		private final HelenusProperty p1;

		public Mapper1(ColumnValueProvider provider, HelenusPropertyNode p1) {
			this.provider = provider;
			this.p1 = p1.getProperty();
		}

		@Override
		public Fun.Tuple1<A> apply(Row row) {
			return new Fun.Tuple1<A>(provider.getColumnValue(row, 0, p1));
		}
	}

	public static final class Mapper2<A, B> implements Function<Row, Fun.Tuple2<A, B>> {

		private final ColumnValueProvider provider;
		private final HelenusProperty p1;
		private final HelenusProperty p2;

		public Mapper2(ColumnValueProvider provider, HelenusPropertyNode p1, HelenusPropertyNode p2) {
			this.provider = provider;
			this.p1 = p1.getProperty();
			this.p2 = p2.getProperty();
		}

		@Override
		public Fun.Tuple2<A, B> apply(Row row) {
			return new Fun.Tuple2<A, B>(provider.getColumnValue(row, 0, p1), provider.getColumnValue(row, 1, p2));
		}
	}

	public static final class Mapper3<A, B, C> implements Function<Row, Fun.Tuple3<A, B, C>> {

		private final ColumnValueProvider provider;
		private final HelenusProperty p1;
		private final HelenusProperty p2;
		private final HelenusProperty p3;

		public Mapper3(ColumnValueProvider provider, HelenusPropertyNode p1, HelenusPropertyNode p2,
				HelenusPropertyNode p3) {
			this.provider = provider;
			this.p1 = p1.getProperty();
			this.p2 = p2.getProperty();
			this.p3 = p3.getProperty();
		}

		@Override
		public Fun.Tuple3<A, B, C> apply(Row row) {
			return new Fun.Tuple3<A, B, C>(provider.getColumnValue(row, 0, p1), provider.getColumnValue(row, 1, p2),
					provider.getColumnValue(row, 2, p3));
		}
	}

	public static final class Mapper4<A, B, C, D> implements Function<Row, Fun.Tuple4<A, B, C, D>> {

		private final ColumnValueProvider provider;
		private final HelenusProperty p1;
		private final HelenusProperty p2;
		private final HelenusProperty p3;
		private final HelenusProperty p4;

		public Mapper4(ColumnValueProvider provider, HelenusPropertyNode p1, HelenusPropertyNode p2,
				HelenusPropertyNode p3, HelenusPropertyNode p4) {
			this.provider = provider;
			this.p1 = p1.getProperty();
			this.p2 = p2.getProperty();
			this.p3 = p3.getProperty();
			this.p4 = p4.getProperty();
		}

		@Override
		public Fun.Tuple4<A, B, C, D> apply(Row row) {
			return new Fun.Tuple4<A, B, C, D>(provider.getColumnValue(row, 0, p1), provider.getColumnValue(row, 1, p2),
					provider.getColumnValue(row, 2, p3), provider.getColumnValue(row, 3, p4));
		}
	}

	public static final class Mapper5<A, B, C, D, E> implements Function<Row, Fun.Tuple5<A, B, C, D, E>> {

		private final ColumnValueProvider provider;
		private final HelenusProperty p1, p2, p3, p4, p5;

		public Mapper5(ColumnValueProvider provider, HelenusPropertyNode p1, HelenusPropertyNode p2,
				HelenusPropertyNode p3, HelenusPropertyNode p4, HelenusPropertyNode p5) {
			this.provider = provider;
			this.p1 = p1.getProperty();
			this.p2 = p2.getProperty();
			this.p3 = p3.getProperty();
			this.p4 = p4.getProperty();
			this.p5 = p5.getProperty();
		}

		@Override
		public Fun.Tuple5<A, B, C, D, E> apply(Row row) {
			return new Fun.Tuple5<A, B, C, D, E>(provider.getColumnValue(row, 0, p1),
					provider.getColumnValue(row, 1, p2), provider.getColumnValue(row, 2, p3),
					provider.getColumnValue(row, 3, p4), provider.getColumnValue(row, 4, p5));
		}
	}

	public static final class Mapper6<A, B, C, D, E, F> implements Function<Row, Fun.Tuple6<A, B, C, D, E, F>> {

		private final ColumnValueProvider provider;
		private final HelenusProperty p1, p2, p3, p4, p5, p6;

		public Mapper6(ColumnValueProvider provider, HelenusPropertyNode p1, HelenusPropertyNode p2,
				HelenusPropertyNode p3, HelenusPropertyNode p4, HelenusPropertyNode p5, HelenusPropertyNode p6) {
			this.provider = provider;
			this.p1 = p1.getProperty();
			this.p2 = p2.getProperty();
			this.p3 = p3.getProperty();
			this.p4 = p4.getProperty();
			this.p5 = p5.getProperty();
			this.p6 = p6.getProperty();
		}

		@Override
		public Fun.Tuple6<A, B, C, D, E, F> apply(Row row) {
			return new Fun.Tuple6<A, B, C, D, E, F>(provider.getColumnValue(row, 0, p1),
					provider.getColumnValue(row, 1, p2), provider.getColumnValue(row, 2, p3),
					provider.getColumnValue(row, 3, p4), provider.getColumnValue(row, 4, p5),
					provider.getColumnValue(row, 5, p6));
		}
	}

	public static final class Mapper7<A, B, C, D, E, F, G> implements Function<Row, Fun.Tuple7<A, B, C, D, E, F, G>> {

		private final ColumnValueProvider provider;
		private final HelenusProperty p1, p2, p3, p4, p5, p6, p7;

		public Mapper7(ColumnValueProvider provider, HelenusPropertyNode p1, HelenusPropertyNode p2,
				HelenusPropertyNode p3, HelenusPropertyNode p4, HelenusPropertyNode p5, HelenusPropertyNode p6,
				HelenusPropertyNode p7) {
			this.provider = provider;
			this.p1 = p1.getProperty();
			this.p2 = p2.getProperty();
			this.p3 = p3.getProperty();
			this.p4 = p4.getProperty();
			this.p5 = p5.getProperty();
			this.p6 = p6.getProperty();
			this.p7 = p7.getProperty();
		}

		@Override
		public Fun.Tuple7<A, B, C, D, E, F, G> apply(Row row) {
			return new Fun.Tuple7<A, B, C, D, E, F, G>(provider.getColumnValue(row, 0, p1),
					provider.getColumnValue(row, 1, p2), provider.getColumnValue(row, 2, p3),
					provider.getColumnValue(row, 3, p4), provider.getColumnValue(row, 4, p5),
					provider.getColumnValue(row, 5, p6), provider.getColumnValue(row, 6, p7));
		}
	}
}
