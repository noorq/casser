package casser.core.operation;

import java.util.concurrent.Future;
import java.util.stream.Stream;

import casser.core.AbstractSessionOperations;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.BuiltStatement;

public abstract class AbstractObjectOperation<E, O extends AbstractObjectOperation<E, O>> extends AbstractOperation<E, O> {

	public abstract BuiltStatement buildStatement();
	
	public abstract E transform(ResultSet resultSet);
	
	public AbstractObjectOperation(AbstractSessionOperations sessionOperations) {
		super(sessionOperations);
	}
	
	public E sync() {
		
		ResultSet resultSet = sessionOperations.execute(buildStatement());
		
		System.out.println("resultSet = " + resultSet);
		
		return transform(resultSet);
	}
	
	public Future<E> async() {
		return null;
	}
	
	
}
