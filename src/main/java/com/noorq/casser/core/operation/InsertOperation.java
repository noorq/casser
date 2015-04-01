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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.noorq.casser.core.AbstractSessionOperations;
import com.noorq.casser.core.Getter;
import com.noorq.casser.core.reflect.CasserPropertyNode;
import com.noorq.casser.core.tuple.Tuple2;
import com.noorq.casser.mapping.CasserMappingEntity;
import com.noorq.casser.mapping.CasserMappingProperty;
import com.noorq.casser.mapping.MappingUtil;
import com.noorq.casser.mapping.value.BeanColumnValueProvider;
import com.noorq.casser.support.CasserMappingException;

public final class InsertOperation extends AbstractOperation<ResultSet, InsertOperation> {

	private final List<Tuple2<CasserPropertyNode, Object>> values = new ArrayList<Tuple2<CasserPropertyNode, Object>>();
	private final boolean ifNotExists;

	public InsertOperation(AbstractSessionOperations sessionOperations, boolean ifNotExists) {
		super(sessionOperations);
		
		this.ifNotExists = ifNotExists;
	}
	
	public InsertOperation(AbstractSessionOperations sessionOperations, CasserMappingEntity entity, Object pojo, boolean ifNotExists) {
		super(sessionOperations);
		
		this.ifNotExists = ifNotExists;
		
		for (CasserMappingProperty prop : entity.getMappingProperties()) {
			
			Object value = BeanColumnValueProvider.INSTANCE.getColumnValue(pojo, -1, prop);
			
			value = sessionOps.getValuePreparer().prepareColumnValue(value, prop);
			
			if (value != null) {
				
				CasserPropertyNode node = new CasserPropertyNode(prop, Optional.empty());
				values.add(Tuple2.of(node, value));
			}
			
		}
		
	}

	public <V> InsertOperation value(Getter<V> getter, V val) {
		
		Objects.requireNonNull(getter, "getter is empty");
		
		if (val != null) {
			CasserPropertyNode node = MappingUtil.resolveMappingProperty(getter);
			
			Object value = sessionOps.getValuePreparer().prepareColumnValue(val, node.getProperty());
			
			if (value != null) {
				values.add(Tuple2.of(node, value));
			}
		}
		
		return this;
	}
	
	@Override
	public BuiltStatement buildStatement() {
		
		CasserMappingEntity entity = null;
		
		for (Tuple2<CasserPropertyNode, Object> tuple : values) {
			
			if (entity == null) {
				entity = tuple._1.getEntity();
			}
			else if (entity != tuple._1.getEntity()) {
				throw new CasserMappingException("you can select columns only from a single entity " + entity + " or " + tuple._1.getEntity());
			}
		}
		
		if (entity == null) {
			throw new CasserMappingException("no entity or table to select data");
		}
		
		Insert insert = QueryBuilder.insertInto(entity.getName().toCql());
		
		if (ifNotExists) {
			insert.ifNotExists();
		}
		
		values.forEach(t -> {
			insert.value(t._1.getColumnName(), t._2);
		});
		
		return insert;
	}

	@Override
	public ResultSet transform(ResultSet resultSet) {
		return resultSet;
	}
	
	
	
}
