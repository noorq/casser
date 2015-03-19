package casser.core.operation;

import java.util.concurrent.Future;
import java.util.stream.Stream;

import com.datastax.driver.core.ResultSet;

import casser.core.AbstractSessionOperations;

public abstract class AbstractStreamOperation<E, O extends AbstractStreamOperation<E, O>> extends AbstractOperation<E, O> {

	public AbstractStreamOperation(AbstractSessionOperations sessionOperations) {
		super(sessionOperations);
	}
	
	public abstract Stream<E> transform(ResultSet resultSet);
	
	public Stream<E> sync() {
		
		ResultSet resultSet = sessionOperations.execute(buildStatement());
		
		System.out.println("resultSet = " + resultSet);
		
		return transform(resultSet);
	}
	
	public Future<Stream<E>> async() {
		return null;
	}
	
	
}
