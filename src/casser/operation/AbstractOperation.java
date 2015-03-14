package casser.operation;

import java.util.concurrent.Future;

import casser.core.PreparedStatement;

public abstract class AbstractOperation<E, O extends AbstractOperation<E, O>> {

	public PreparedStatement<E, O> prepare() {
		return null;
	}
	
	public E sync() {
		return null;
	}
	
	public Future<E> async() {
		return null;
	}
	
	
}
