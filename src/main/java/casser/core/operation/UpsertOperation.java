package casser.core.operation;

import casser.mapping.CasserMappingEntity;

import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class UpsertOperation extends AbstractOperation<Object, UpsertOperation> {

	public UpsertOperation(CasserMappingEntity<?> entity, Object pojo) {
		
		Insert insert = QueryBuilder.insertInto(entity.getTableName());
		
		
		
	}
	
}
