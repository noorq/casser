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

import casser.mapping.CasserMappingProperty;
import casser.mapping.ColumnValueProvider;

public final class Tuple6<V1, V2, V3, V4, V5, V6> {

	public final V1 v1;
	public final V2 v2;
	public final V3 v3;
	public final V4 v4;
	public final V5 v5;
	public final V6 v6;
	
	public Tuple6(V1 v1, V2 v2, V3 v3, V4 v4, V5 v5, V6 v6) {
		this.v1 = v1;
		this.v2 = v2;
		this.v3 = v3;
		this.v4 = v4;
		this.v5 = v5;
		this.v6 = v6;
	}
	
	public final static class Mapper<V1, V2, V3, V4, V5, V6> implements 
		Function<ColumnValueProvider, 
		Tuple6<V1, V2, V3, V4, V5, V6>> {

		private final CasserMappingProperty<?> p1, p2, p3, p4, p5, p6;
		
		public Mapper(
				CasserMappingProperty<?> p1, 
				CasserMappingProperty<?> p2, 
				CasserMappingProperty<?> p3,
				CasserMappingProperty<?> p4,
				CasserMappingProperty<?> p5,
				CasserMappingProperty<?> p6
				) {
			this.p1 = p1;
			this.p2 = p2;
			this.p3 = p3;
			this.p4 = p4;
			this.p5 = p5;
			this.p6 = p6;
		}
		
		@Override
		public Tuple6<V1, V2, V3, V4, V5, V6> apply(ColumnValueProvider provider) {
			return new Tuple6<V1, V2, V3, V4, V5, V6>(
					provider.getColumnValue(0, p1), 
					provider.getColumnValue(1, p2),
					provider.getColumnValue(2, p3),
					provider.getColumnValue(3, p4),
					provider.getColumnValue(4, p5),
					provider.getColumnValue(5, p6)
					);
		}
	}

}
