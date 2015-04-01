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

public final class Tuple3<A, B, C> {

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
	
	public final static class Mapper<A, B, C> implements Function<Row, Tuple3<A, B, C>> {

		private final ColumnValueProvider provider;
		private final CasserMappingProperty p1;
		private final CasserMappingProperty p2;
		private final CasserMappingProperty p3;
		
		public Mapper(ColumnValueProvider provider, 
				CasserPropertyNode p1, 
				CasserPropertyNode p2, 
				CasserPropertyNode p3) {
			this.provider = provider;
			this.p1 = p1.getProperty();
			this.p2 = p2.getProperty();
			this.p3 = p3.getProperty();
		}
		
		@Override
		public Tuple3<A, B, C> apply(Row row) {
			return new Tuple3<A, B, C>(
					provider.getColumnValue(row, 0, p1), 
					provider.getColumnValue(row, 1, p2),
					provider.getColumnValue(row, 2, p3)
					);
		}
	}

	@Override
	public String toString() {
		return "Tuple3 [_1=" + _1 + ", _2=" + _2 + ", _3=" + _3 + "]";
	}

	
	
}
