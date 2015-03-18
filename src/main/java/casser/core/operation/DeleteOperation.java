package casser.core.operation;

import casser.core.AbstractSessionOperations;

import com.datastax.driver.core.querybuilder.BuiltStatement;


public class DeleteOperation extends AbstractFilterOperation<Object, DeleteOperation> {

	public DeleteOperation(AbstractSessionOperations sessionOperations) {
		super(sessionOperations);
	}
	
	@Override
	public BuiltStatement getBuiltStatement() {
		return null;
	}

}
