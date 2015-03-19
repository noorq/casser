package casser.core.operation;

import casser.core.AbstractSessionOperations;
import casser.core.Filter;
import casser.mapping.CasserMappingEntity;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Delete.Where;
import com.datastax.driver.core.querybuilder.QueryBuilder;


public class DeleteOperation extends AbstractFilterOperation<ResultSet, DeleteOperation> {

	private final CasserMappingEntity<?> entity;
	
	public DeleteOperation(AbstractSessionOperations sessionOperations, CasserMappingEntity<?> entity) {
		super(sessionOperations);
		
		this.entity = entity;
	}
	
	@Override
	public BuiltStatement buildStatement() {
		
		if (filters != null && !filters.isEmpty()) {

			Delete delete = QueryBuilder.delete().from(entity.getTableName());
			
			Where where = delete.where();
			
			for (Filter<?> filter : filters) {
				where.and(filter.getClause());
			}
			
			return delete;

		}
		else {
			return QueryBuilder.truncate(entity.getTableName());
		}
	}

	@Override
	public ResultSet transform(ResultSet resultSet) {
		return resultSet;
	}
	
}
