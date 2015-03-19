package casser.core.operation;

import casser.core.AbstractSessionOperations;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.BuiltStatement;


public class UpdateOperation extends AbstractFilterOperation<ResultSet, UpdateOperation> {
	
	public UpdateOperation(AbstractSessionOperations sessionOperations) {
		super(sessionOperations);
	}
	
	@Override
	public BuiltStatement buildStatement() {
		return null;
	}

	@Override
	public ResultSet transform(ResultSet resultSet) {
		return resultSet;
	}
	

}
