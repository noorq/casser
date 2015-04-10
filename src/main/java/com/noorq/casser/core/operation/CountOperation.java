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
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.noorq.casser.core.AbstractSessionOperations;
import com.noorq.casser.core.Filter;
import com.noorq.casser.core.reflect.CasserPropertyNode;
import com.noorq.casser.mapping.CasserMappingEntity;
import com.noorq.casser.support.CasserMappingException;

public final class CountOperation extends AbstractFilterOperation<Long, CountOperation> {

	private CasserMappingEntity entity;
	
	public CountOperation(AbstractSessionOperations sessionOperations) {
		super(sessionOperations);
	}
	
	public CountOperation(AbstractSessionOperations sessionOperations, CasserMappingEntity entity) {
		super(sessionOperations);
		
		this.entity = entity;
	}

	@Override
	public BuiltStatement buildStatement() {
		
		if (filters != null && !filters.isEmpty()) {
			filters.forEach(f -> addPropertyNode(f.getNode()));
		}
		
		if (entity == null) {
			throw new CasserMappingException("unknown entity");
		}
		
		Select select = QueryBuilder.select().countAll().from(entity.getName().toCql());
		
		if (filters != null && !filters.isEmpty()) {
		
			Where where = select.where();
			
			for (Filter<?> filter : filters) {
				where.and(filter.getClause(sessionOps.getValuePreparer()));
			}
		}
		
		return select;
	}
	
	@Override
	public Long transform(ResultSet resultSet) {
		return resultSet.one().getLong(0);
	}
	
	private void addPropertyNode(CasserPropertyNode p) {
		if (entity == null) {
			entity = p.getEntity();
		}
		else if (entity != p.getEntity()) {
			throw new CasserMappingException("you can count columns only in single entity " + entity.getMappingInterface() + " or " + p.getEntity().getMappingInterface());
		}
	}

}
