package casser.core.operation;

import casser.core.AbstractSessionOperations;
import casser.core.Prepared;

import com.datastax.driver.core.querybuilder.BuiltStatement;

public abstract class AbstractOperation<E, O extends AbstractOperation<E, O>> {

	protected final AbstractSessionOperations sessionOperations;
	
	public abstract BuiltStatement buildStatement();
	
	public AbstractOperation(AbstractSessionOperations sessionOperations) {
		this.sessionOperations = sessionOperations;
	}
	
	public String cql() {
		return buildStatement().setForceNoValues(true).getQueryString();
	}
	
	public Prepared<O> prepare() {
		return null;
	}
	
}
