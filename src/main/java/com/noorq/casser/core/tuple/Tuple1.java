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

public final class Tuple1<V1> {

	public final V1 v1;

	public Tuple1(V1 v1) {
		this.v1 = v1;
	}

	public final static class Mapper<V1> implements Function<Row, Tuple1<V1>> {

		private final ColumnValueProvider provider;
		private final CasserMappingProperty p1;
		
		public Mapper(ColumnValueProvider provider, CasserPropertyNode p1) {
			this.provider = provider;
			this.p1 = p1.getProperty();
		}
		
		@Override
		public Tuple1<V1> apply(Row row) {
			return new Tuple1<V1>(provider.getColumnValue(row, 0, p1));
		}
	}


	@Override
	public String toString() {
		return "Tuple1 [v1=" + v1 + "]";
	}
	
	
}
