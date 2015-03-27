/*
 *      Copyright (C) 2015 Noorq, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package casser.core.operation;

import casser.core.AbstractSessionOperations;
import casser.core.Filter;
import casser.mapping.CasserMappingEntity;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Delete.Where;
import com.datastax.driver.core.querybuilder.QueryBuilder;


public final class DeleteOperation extends AbstractFilterOperation<ResultSet, DeleteOperation> {

	private final CasserMappingEntity entity;
	
	public DeleteOperation(AbstractSessionOperations sessionOperations, CasserMappingEntity entity) {
		super(sessionOperations);
		
		this.entity = entity;
	}
	
	@Override
	public BuiltStatement buildStatement() {
		
		if (filters != null && !filters.isEmpty()) {

			Delete delete = QueryBuilder.delete().from(entity.getName());
			
			Where where = delete.where();
			
			for (Filter<?> filter : filters) {
				where.and(filter.getClause(sessionOps.getValuePreparer()));
			}
			
			return delete;

		}
		else {
			return QueryBuilder.truncate(entity.getName());
		}
	}

	@Override
	public ResultSet transform(ResultSet resultSet) {
		return resultSet;
	}
	
}
