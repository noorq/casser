package casser.operation;

import java.util.function.Function;

import casser.dsl.Getter;


public class SelectOperation<E> extends AbstractStreamOperation<E> {

	public <V> SelectOperation<E> where(Getter<V> getter, String operator, V val) {
		return this;
	}

	public <V> SelectOperation<E> and(Getter<V> getter, String operator, V val) {
		return this;
	}

	public <R> SelectOperation<R> map(Function<E, R> fn) {
		return null;
	}
	
}
