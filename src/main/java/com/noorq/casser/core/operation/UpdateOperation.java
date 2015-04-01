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

import java.util.Arrays;
import java.util.Objects;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.noorq.casser.core.AbstractSessionOperations;
import com.noorq.casser.core.Filter;
import com.noorq.casser.core.Getter;
import com.noorq.casser.core.reflect.CasserPropertyNode;
import com.noorq.casser.mapping.CasserMappingEntity;
import com.noorq.casser.mapping.MappingUtil;
import com.noorq.casser.support.CasserMappingException;


public final class UpdateOperation extends AbstractFilterOperation<ResultSet, UpdateOperation> {
	
	private final CasserPropertyNode[] props;
	private final Object[] vals;

	public UpdateOperation(AbstractSessionOperations sessionOperations) {
		super(sessionOperations);
		
		this.props = new CasserPropertyNode[0];
		this.vals = new Object[0];
	}
	
	public UpdateOperation(AbstractSessionOperations sessionOperations, CasserPropertyNode p, Object v) {
		super(sessionOperations);
		
		this.props = new CasserPropertyNode[1];
		this.vals = new Object[1];
		
		this.props[0] = p;
		this.vals[0] = v;
	}

	public UpdateOperation(UpdateOperation other, CasserPropertyNode p, Object v) {
		super(other.sessionOps);
		
		this.props = Arrays.copyOf(other.props, other.props.length + 1);
		this.vals = Arrays.copyOf(other.vals, other.vals.length + 1);
		
		this.props[other.props.length] = p;
		this.vals[other.vals.length] = v;
	}
	
	public <V> UpdateOperation set(Getter<V> getter, V v) {
		Objects.requireNonNull(getter, "field is empty");
		Objects.requireNonNull(v, "value is empty");

		CasserPropertyNode p = MappingUtil.resolveMappingProperty(getter);
		
		return new UpdateOperation(this, p, v);
	}
	
	@Override
	public BuiltStatement buildStatement() {
		
		CasserMappingEntity entity = null;
		
		for (CasserPropertyNode prop : props) {
			if (entity == null) {
				entity = prop.getEntity();
			}
			else if (entity != prop.getEntity()) {
				throw new CasserMappingException("you can update columns only for a single entity " + entity.getMappingInterface() + " or " + prop.getEntity().getMappingInterface());
			}
		}
		
		if (entity == null) {
			throw new CasserMappingException("no entity or table to update data");
		}
		
		
		Update update = QueryBuilder.update(entity.getName().toCql());
		
		for (int i = 0; i != props.length; ++i) {
			
			Object value = sessionOps.getValuePreparer().prepareColumnValue(vals[i], props[i].getProperty());
			
			update.with(QueryBuilder.set(props[i].getColumnName(), value));
		
		}
		
		if (filters != null && !filters.isEmpty()) {
			
			for (Filter<?> filter : filters) {
				update.where(filter.getClause(sessionOps.getValuePreparer()));
			}
		}
		
		return update;
	}

	@Override
	public ResultSet transform(ResultSet resultSet) {
		return resultSet;
	}
	

}
