package casser.core.operation;

import casser.core.AbstractSessionOperations;

import com.datastax.driver.core.querybuilder.BuiltStatement;

public class CountOperation extends AbstractOperation<Long, CountOperation> {

	public CountOperation(AbstractSessionOperations sessionOperations) {
		super(sessionOperations);
	}

	@Override
	public BuiltStatement getBuiltStatement() {
		return null;
	}
	
	
}
