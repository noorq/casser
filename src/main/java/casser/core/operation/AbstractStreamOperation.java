package casser.core.operation;

import java.util.concurrent.Future;
import java.util.stream.Stream;

import casser.core.AbstractSessionOperations;
import casser.core.Prepared;

public abstract class AbstractStreamOperation<E, O extends AbstractStreamOperation<E, O>> {

	private final AbstractSessionOperations sessionOperations;
	
	public AbstractStreamOperation(AbstractSessionOperations sessionOperations) {
		this.sessionOperations = sessionOperations;
	}
	
	public String cql() {
		return null;
	}
	
	public Prepared<O> prepare() {
		return null;
	}
	
	public Stream<E> sync() {
		return null;
	}
	
	public Future<Stream<E>> async() {
		return null;
	}
	
	
}
