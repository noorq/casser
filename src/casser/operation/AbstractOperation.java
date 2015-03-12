package casser.operation;

import java.util.concurrent.Future;
import java.util.stream.Stream;

public abstract class AbstractOperation<E> {

	public Stream<E> sync() {
		return null;
	}
	
	public Future<Stream<E>> async() {
		return null;
	}
	
	
}
