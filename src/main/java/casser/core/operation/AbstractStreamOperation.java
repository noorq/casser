package casser.core.operation;

import java.util.concurrent.Future;
import java.util.stream.Stream;

import casser.core.Prepared;

public abstract class AbstractStreamOperation<E, O extends AbstractStreamOperation<E, O>> {

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
