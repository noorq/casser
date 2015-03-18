package casser.core.operation;

import casser.core.AbstractSessionOperations;

import com.datastax.driver.core.querybuilder.BuiltStatement;


public class UpdateOperation extends AbstractFilterOperation<Object, UpdateOperation> {
	
	public UpdateOperation(AbstractSessionOperations sessionOperations) {
		super(sessionOperations);
	}
	
	@Override
	public BuiltStatement getBuiltStatement() {
		return null;
	}


}
