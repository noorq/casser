package casser.core.operation;

import casser.core.AbstractSessionOperations;
import casser.mapping.CasserMappingEntity;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;


public class DeleteOperation extends AbstractFilterOperation<ResultSet, DeleteOperation> {

	private final CasserMappingEntity<?> entity;
	
	public DeleteOperation(AbstractSessionOperations sessionOperations, CasserMappingEntity<?> entity) {
		super(sessionOperations);
		
		this.entity = entity;
	}
	
	@Override
	public BuiltStatement getBuiltStatement() {
		
		return QueryBuilder.truncate(entity.getTableName());
		
		//return QueryBuilder.delete().from(entity.getTableName());
	}

	@Override
	public ResultSet transform(ResultSet resultSet) {
		return resultSet;
	}
	
}
