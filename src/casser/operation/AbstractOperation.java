package casser.operation;

import java.util.concurrent.Future;

public abstract class AbstractOperation<E> {

	public E sync() {
		return null;
	}
	
	public Future<E> async() {
		return null;
	}
	
	
}
