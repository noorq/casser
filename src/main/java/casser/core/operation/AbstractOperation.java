package casser.core.operation;

import java.util.concurrent.Future;

import casser.core.AbstractSessionOperations;
import casser.core.Prepared;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.BuiltStatement;

public abstract class AbstractOperation<E, O extends AbstractOperation<E, O>> {

	private final AbstractSessionOperations sessionOperations;
	
	public abstract BuiltStatement getBuiltStatement();
	
	public AbstractOperation(AbstractSessionOperations sessionOperations) {
		this.sessionOperations = sessionOperations;
	}
	
	public String cql() {
		return getBuiltStatement().setForceNoValues(true).getQueryString();
	}
	
	public Prepared<O> prepare() {
		return null;
	}
	
	public E sync() {
		
		ResultSet resultSet = sessionOperations.execute(getBuiltStatement());
		
		System.out.println("resultSet = " + resultSet);
		
		return null;
	}
	
	public Future<E> async() {
		return null;
	}
	
	
}
