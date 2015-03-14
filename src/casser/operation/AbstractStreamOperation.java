package casser.operation;

import java.util.concurrent.Future;
import java.util.stream.Stream;

import casser.core.PreparedStreamStatement;

public abstract class AbstractStreamOperation<E, O extends AbstractStreamOperation<E, O>> {

	public PreparedStreamStatement<E, O> prepare() {
		return null;
	}
	
	public Stream<E> sync() {
		return null;
	}
	
	public Future<Stream<E>> async() {
		return null;
	}
	
	
}
