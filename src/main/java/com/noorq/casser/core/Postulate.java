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
package com.noorq.casser.core;

import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.noorq.casser.core.reflect.CasserPropertyNode;
import com.noorq.casser.mapping.value.ColumnValuePreparer;
import com.noorq.casser.support.CasserMappingException;

public final class Postulate<V> {

	private final Operator operator;
	private final V value;
	private final V[] values;
	
	protected Postulate(Operator op, V value) {
		this.operator = op;
		this.value = value;
		this.values = null;
		
		if (op == Operator.IN) {
			throw new IllegalArgumentException("invalid usage of the 'in' operator");
		}
	}

	protected Postulate(Operator op, V[] values) {
		this.operator = op;
		this.value = null;
		this.values = values;

		if (op != Operator.IN) {
			throw new IllegalArgumentException("invalid usage of the non 'in' operator");
		}
    }
	
	public Clause getClause(CasserPropertyNode node, ColumnValuePreparer valuePreparer) {
		
		switch(operator) {
		
		case EQ:
			return QueryBuilder.eq(node.getColumnName(), 
					valuePreparer.prepareColumnValue(value, node.getProperty()));
		
		case IN:
			Object[] preparedValues = new Object[values.length];
			for (int i = 0; i != values.length; ++i) {
				preparedValues[i] = valuePreparer.prepareColumnValue(values[i], node.getProperty());
			}
			return QueryBuilder.in(node.getColumnName(), preparedValues);
			
		case LT:
			return QueryBuilder.lt(node.getColumnName(), 
					valuePreparer.prepareColumnValue(value, node.getProperty()));

		case LTE:
			return QueryBuilder.lte(node.getColumnName(), 
					valuePreparer.prepareColumnValue(value, node.getProperty()));

		case GT:
			return QueryBuilder.gt(node.getColumnName(), 
					valuePreparer.prepareColumnValue(value, node.getProperty()));

		case GTE:
			return QueryBuilder.gte(node.getColumnName(), 
					valuePreparer.prepareColumnValue(value, node.getProperty()));

		default:
			throw new CasserMappingException("unknown filter operation " + operator);
		}
		
	}

	@Override
	public String toString() {
		
		if (operator == Operator.IN) {
			return "in(" + values + ")";
		}
		
		return operator.getName() + value;
		
	}		
	
	
}
