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

public final class Tuple7<A, B, C, D, E, F, G> {

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
	
	public final static class Mapper<A, B, C, D, E, F, G> implements 
		Function<Row, 
		Tuple7<A, B, C, D, E, F, G>> {

		private final ColumnValueProvider provider;
		private final CasserMappingProperty p1, p2, p3, p4, p5, p6, p7;
		
		public Mapper(ColumnValueProvider provider, 
				CasserPropertyNode p1, 
				CasserPropertyNode p2, 
				CasserPropertyNode p3,
				CasserPropertyNode p4,
				CasserPropertyNode p5,
				CasserPropertyNode p6,
				CasserPropertyNode p7
				) {
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
		public Tuple7<A, B, C, D, E, F, G> apply(Row row) {
			return new Tuple7<A, B, C, D, E, F, G>(
					provider.getColumnValue(row, 0, p1), 
					provider.getColumnValue(row, 1, p2),
					provider.getColumnValue(row, 2, p3),
					provider.getColumnValue(row, 3, p4),
					provider.getColumnValue(row, 4, p5),
					provider.getColumnValue(row, 5, p6),
					provider.getColumnValue(row, 6, p7)
					);
		}
	}

	@Override
	public String toString() {
		return "Tuple7 [_1=" + _1 + ", _2=" + _2 + ", _3=" + _3 + ", _4=" + _4
				+ ", _5=" + _5 + ", _6=" + _6 + ", _7=" + _7 + "]";
	}

	
	
}
