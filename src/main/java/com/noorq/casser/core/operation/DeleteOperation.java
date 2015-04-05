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
package com.noorq.casser.core.operation;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Delete.Where;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.noorq.casser.core.AbstractSessionOperations;
import com.noorq.casser.core.Filter;
import com.noorq.casser.mapping.CasserMappingEntity;


public final class DeleteOperation extends AbstractFilterOperation<ResultSet, DeleteOperation> {

	private final CasserMappingEntity entity;
	
	private boolean ifExists = false;
	
	private int[] ttl;
	private long[] timestamp;
	
	public DeleteOperation(AbstractSessionOperations sessionOperations, CasserMappingEntity entity) {
		super(sessionOperations);
		
		this.entity = entity;
	}
	
	@Override
	public BuiltStatement buildStatement() {
		
		if (filters != null && !filters.isEmpty()) {

			Delete delete = QueryBuilder.delete().from(entity.getName().toCql());
			
			if (this.ifExists) {
				delete.ifExists();
			}
			
			Where where = delete.where();
			
			for (Filter<?> filter : filters) {
				where.and(filter.getClause(sessionOps.getValuePreparer()));
			}
			
			if (this.ttl != null) {
				delete.using(QueryBuilder.ttl(this.ttl[0]));
			}
			if (this.timestamp != null) {
				delete.using(QueryBuilder.timestamp(this.timestamp[0]));
			}

			return delete;

		}
		else {
			return QueryBuilder.truncate(entity.getName().toCql());
		}
	}

	@Override
	public ResultSet transform(ResultSet resultSet) {
		return resultSet;
	}
	
	public DeleteOperation ifExists() {
		this.ifExists = true;
		return this;
	}
	
	public DeleteOperation usingTtl(int ttl) {
		this.ttl = new int[1];
		this.ttl[0] = ttl;
		return this;
	}

	public DeleteOperation usingTimestamp(long timestamp) {
		this.timestamp = new long[1];
		this.timestamp[0] = timestamp;
		return this;
	}
}
