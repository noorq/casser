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
package casser.core.tuple;

import java.util.function.Function;

import casser.core.reflect.CasserPropertyNode;
import casser.mapping.CasserMappingProperty;
import casser.mapping.value.ColumnValueProvider;

import com.datastax.driver.core.Row;

public final class Tuple7<V1, V2, V3, V4, V5, V6, V7> {

	public final V1 v1;
	public final V2 v2;
	public final V3 v3;
	public final V4 v4;
	public final V5 v5;
	public final V6 v6;
	public final V7 v7;
	
	public Tuple7(V1 v1, V2 v2, V3 v3, V4 v4, V5 v5, V6 v6, V7 v7) {
		this.v1 = v1;
		this.v2 = v2;
		this.v3 = v3;
		this.v4 = v4;
		this.v5 = v5;
		this.v6 = v6;
		this.v7 = v7;
	}
	
	public final static class Mapper<V1, V2, V3, V4, V5, V6, V7> implements 
		Function<Row, 
		Tuple7<V1, V2, V3, V4, V5, V6, V7>> {

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
		public Tuple7<V1, V2, V3, V4, V5, V6, V7> apply(Row row) {
			return new Tuple7<V1, V2, V3, V4, V5, V6, V7>(
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
		return "Tuple7 [v1=" + v1 + ", v2=" + v2 + ", v3=" + v3 + ", v4=" + v4
				+ ", v5=" + v5 + ", v6=" + v6 + ", v7=" + v7 + "]";
	}

	
	
}
