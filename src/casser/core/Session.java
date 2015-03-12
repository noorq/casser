package casser.core;

import casser.dsl.Getter;
import casser.dsl.Setter;
import casser.operation.SelectOperation;
import casser.operation.UpdateOperation;
import casser.tuple.Tuple1;
import casser.tuple.Tuple2;
import casser.tuple.Tuple3;

public class Session {

	public <V1> SelectOperation<Tuple1<V1>> select(Getter<V1> getter1) {
		return null;
	}

	public <V1, V2> SelectOperation<Tuple2<V1, V2>> select(Getter<V1> getter1, Getter<V2> getter2) {
		return null;
	}

	public <V1, V2, V3> SelectOperation<Tuple3<V1, V2, V3>> select(Getter<V1> getter1, Getter<V2> getter2, Getter<V3> getter3) {
		return null;
	}

	public <V1> UpdateOperation update(Setter<V1> setter1, V1 v1) {
		return null;
	}
	
	public void close() {
		
	}
}
