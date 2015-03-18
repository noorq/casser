package casser.core.operation;

import casser.core.AbstractSessionOperations;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.BuiltStatement;

public class CountOperation extends AbstractObjectOperation<Long, CountOperation> {

	public CountOperation(AbstractSessionOperations sessionOperations) {
		super(sessionOperations);
	}

	@Override
	public BuiltStatement getBuiltStatement() {
		return null;
	}
	
	@Override
	public Long transform(ResultSet resultSet) {
		return resultSet.one().getLong(0);
	}
	
	
}
