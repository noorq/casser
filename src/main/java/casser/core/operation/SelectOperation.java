package casser.core.operation;

import java.util.function.Function;


public class SelectOperation<E> extends AbstractFilterStreamOperation<E, SelectOperation<E>> {

	public CountOperation count() {
		return null;
	}
	
	public <R> SelectOperation<R> map(Function<E, R> fn) {
		return null;
	}
	
}
