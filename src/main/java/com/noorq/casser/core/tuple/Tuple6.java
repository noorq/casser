/*
 *      Copyright (C) 2015 Noorq, Inc.
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
package com.noorq.casser.core.tuple;

import java.util.function.Function;

import com.datastax.driver.core.Row;
import com.noorq.casser.core.reflect.CasserPropertyNode;
import com.noorq.casser.mapping.CasserMappingProperty;
import com.noorq.casser.mapping.value.ColumnValueProvider;

public final class Tuple6<A, B, C, D, E, F> {

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
	
	public final static class Mapper<A, B, C, D, E, F> implements 
		Function<Row, 
		Tuple6<A, B, C, D, E, F>> {

		private final ColumnValueProvider provider;
		private final CasserMappingProperty p1, p2, p3, p4, p5, p6;
		
		public Mapper(ColumnValueProvider provider, 
				CasserPropertyNode p1, 
				CasserPropertyNode p2, 
				CasserPropertyNode p3,
				CasserPropertyNode p4,
				CasserPropertyNode p5,
				CasserPropertyNode p6
				) {
			this.provider = provider;
			this.p1 = p1.getProperty();
			this.p2 = p2.getProperty();
			this.p3 = p3.getProperty();
			this.p4 = p4.getProperty();
			this.p5 = p5.getProperty();
			this.p6 = p6.getProperty();
		}
		
		@Override
		public Tuple6<A, B, C, D, E, F> apply(Row row) {
			return new Tuple6<A, B, C, D, E, F>(
					provider.getColumnValue(row, 0, p1), 
					provider.getColumnValue(row, 1, p2),
					provider.getColumnValue(row, 2, p3),
					provider.getColumnValue(row, 3, p4),
					provider.getColumnValue(row, 4, p5),
					provider.getColumnValue(row, 5, p6)
					);
		}
	}

	@Override
	public String toString() {
		return "Tuple6 [_1=" + _1 + ", _2=" + _2 + ", _3=" + _3 + ", _4=" + _4
				+ ", _5=" + _5 + ", _6=" + _6 + "]";
	}

	
	
	
}
