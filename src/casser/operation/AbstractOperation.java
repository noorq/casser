package casser.operation;

import java.util.concurrent.Future;

import casser.core.PreparedStreamStatement;

public abstract class AbstractOperation<E> {

	public PreparedStreamStatement<E> prepare() {
		return null;
	}
	
	public E sync() {
		return null;
	}
	
	public Future<E> async() {
		return null;
	}
	
	
}
