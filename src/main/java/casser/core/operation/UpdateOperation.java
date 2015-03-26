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

import java.util.Arrays;
import java.util.Objects;

import casser.core.AbstractSessionOperations;
import casser.core.Filter;
import casser.core.dsl.Setter;
import casser.mapping.CasserMappingEntity;
import casser.mapping.CasserMappingProperty;
import casser.mapping.MappingUtil;
import casser.support.CasserMappingException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;


public final class UpdateOperation extends AbstractFilterOperation<ResultSet, UpdateOperation> {
	
	private final CasserMappingProperty<?>[] props;
	private final Object[] vals;
	
	public UpdateOperation(AbstractSessionOperations sessionOperations, CasserMappingProperty<?> p, Object v) {
		super(sessionOperations);
		
		this.props = new CasserMappingProperty<?>[1];
		this.vals = new Object[1];
		
		this.props[0] = p;
		this.vals[0] = v;
	}

	public UpdateOperation(UpdateOperation other, CasserMappingProperty<?> p, Object v) {
		super(other.sessionOperations);
		
		this.props = Arrays.copyOf(other.props, other.props.length + 1);
		this.vals = Arrays.copyOf(other.vals, other.vals.length + 1);
		
		this.props[other.props.length] = p;
		this.vals[other.vals.length] = v;
	}
	
	public <V> UpdateOperation set(Setter<V> setter, V v) {
		Objects.requireNonNull(setter, "field is empty");
		Objects.requireNonNull(v, "value is empty");

		CasserMappingProperty<?> p = MappingUtil.resolveMappingProperty(setter);
		
		return new UpdateOperation(this, p, v);
	}
	
	@Override
	public BuiltStatement buildStatement() {
		
		CasserMappingEntity<?> entity = null;
		
		for (CasserMappingProperty<?> prop : props) {
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
		
		
		Update update = QueryBuilder.update(entity.getName());
		
		for (int i = 0; i != props.length; ++i) {
			
			Object value = MappingUtil.prepareValueForWrite(props[i], vals[i]);
			
			update.with(QueryBuilder.set(props[i].getColumnName(), value));
		
		}
		
		if (filters != null && !filters.isEmpty()) {
			
			for (Filter<?> filter : filters) {
				update.where(filter.getClause());
			}
		}
		
		return update;
	}

	@Override
	public ResultSet transform(ResultSet resultSet) {
		return resultSet;
	}
	

}
