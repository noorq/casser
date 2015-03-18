package casser.core.operation;

import java.util.function.Function;

import casser.core.AbstractSessionOperations;
import casser.mapping.ColumnValueProvider;


public class SelectOperation<E> extends AbstractFilterStreamOperation<E, SelectOperation<E>> {

	public SelectOperation(AbstractSessionOperations sessionOperations, Function<ColumnValueProvider, E> mapper) {
		super(sessionOperations);
	}
	
	public CountOperation count() {
		return null;
	}
	
	public <R> SelectOperation<R> map(Function<E, R> fn) {
		return null;
	}
	
}
