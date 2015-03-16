package casser.core;

import casser.core.dsl.Getter;

public final class Filter {

	private Filter() {
	}

	public static <V> Filter equal(Getter<V> getter, V val) {
		return null;
	}

	public static <V> Filter notEqual(Getter<V> getter, V val) {
		return null;
	}
	
	public static <V> Filter greater(Getter<V> getter, V val) {
		return null;
	}
	
	public static <V> Filter less(Getter<V> getter, V val) {
		return null;
	}

	public static <V> Filter greaterOrEqual(Getter<V> getter, V val) {
		return null;
	}

	public static <V> Filter lessOrEqual(Getter<V> getter, V val) {
		return null;
	}

}
