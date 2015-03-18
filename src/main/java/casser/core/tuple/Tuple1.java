package casser.core.tuple;

import java.util.function.Function;

import casser.mapping.CasserMappingProperty;
import casser.mapping.ColumnValueProvider;

public final class Tuple1<V1> {

	public final V1 v1;

	public Tuple1(V1 v1) {
		this.v1 = v1;
	}


	public final static class Mapper<V1> implements Function<ColumnValueProvider, Tuple1<V1>> {

		private final CasserMappingProperty<?> p1;
		
		public Mapper(CasserMappingProperty<?> p1) {
			this.p1 = p1;
		}
		
		@Override
		public Tuple1<V1> apply(ColumnValueProvider provider) {
			return new Tuple1<V1>(provider.getColumnValue(0, p1));
		}
	}
	
}
