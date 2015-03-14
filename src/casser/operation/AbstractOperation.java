package casser.operation;

import java.util.concurrent.Future;

import casser.core.Prepared;

public abstract class AbstractOperation<E, O extends AbstractOperation<E, O>> {

	public Prepared<O> prepare() {
		return null;
	}
	
	public E sync() {
		return null;
	}
	
	public Future<E> async() {
		return null;
	}
	
	
}
