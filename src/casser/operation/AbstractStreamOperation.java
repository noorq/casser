package casser.operation;

import java.util.concurrent.Future;
import java.util.stream.Stream;

import casser.core.PreparedStreamStatement;

public abstract class AbstractStreamOperation<E> {

	public PreparedStreamStatement<E> prepare() {
		return null;
	}
	
	public Stream<E> sync() {
		return null;
	}
	
	public Future<Stream<E>> async() {
		return null;
	}
	
	
}
