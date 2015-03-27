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
package casser.core;

import java.util.Objects;

import casser.mapping.CasserMappingProperty;
import casser.mapping.ColumnValuePreparer;
import casser.mapping.MappingUtil;
import casser.support.CasserMappingException;

import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public final class Filter<V> {

	private final CasserMappingProperty property;
	private final Operator operation;
	private final V value;
	private final V[] values;
	
	private Filter(CasserMappingProperty prop, Operator op, V value) {
		this.property = prop;
		this.operation = op;
		this.value = value;
		this.values = null;
	}

	private Filter(CasserMappingProperty prop, Operator op, V[] values) {
		this.property = prop;
		this.operation = op;
		this.value = null;
		this.values = values;
	}
	
	public CasserMappingProperty getProperty() {
		return property;
	}

	public Operator getOperation() {
		return operation;
	}

	public V getValue() {
		return value;
	}

	public Clause getClause(ColumnValuePreparer valuePreparer) {
		
		switch(operation) {
		
		case EQ:
			return QueryBuilder.eq(property.getColumnName(), 
					valuePreparer.prepareColumnValue(value, property));
		
		case IN:
			Object[] preparedValues = new Object[values.length];
			for (int i = 0; i != values.length; ++i) {
				preparedValues[i] = valuePreparer.prepareColumnValue(values[i], property);
			}
			return QueryBuilder.in(property.getColumnName(), preparedValues);
			
		case LT:
			return QueryBuilder.lt(property.getColumnName(), valuePreparer.prepareColumnValue(value, property));

		case LTE:
			return QueryBuilder.lte(property.getColumnName(), valuePreparer.prepareColumnValue(value, property));

		case GT:
			return QueryBuilder.gt(property.getColumnName(), valuePreparer.prepareColumnValue(value, property));

		case GTE:
			return QueryBuilder.gte(property.getColumnName(), valuePreparer.prepareColumnValue(value, property));

		default:
			throw new CasserMappingException("unknown filter operation " + operation);
		}
		
	}
	
	public static <V> Filter<V> equal(Getter<V> getter, V val) {
		return create(getter, Operator.EQ, val);
	}

	public static <V> Filter<V> in(Getter<V> getter, V[] vals) {
		Objects.requireNonNull(getter, "empty getter");
		Objects.requireNonNull(vals, "empty values");
		
		if (vals.length == 0) {
			throw new IllegalArgumentException("values array is empty");
		}
		
		for (int i = 0; i != vals.length; ++i) {
			Objects.requireNonNull(vals[i], "value[" + i + "] is empty");
		}
		
		CasserMappingProperty prop = MappingUtil.resolveMappingProperty(getter);
		
		return new Filter<V>(prop, Operator.IN, vals);
	}
	
	public static <V> Filter<V> greater(Getter<V> getter, V val) {
		return create(getter, Operator.GT, val);
	}
	
	public static <V> Filter<V> less(Getter<V> getter, V val) {
		return create(getter, Operator.LT, val);
	}

	public static <V> Filter<V> greaterOrEqual(Getter<V> getter, V val) {
		return create(getter, Operator.GTE, val);
	}

	public static <V> Filter<V> lessOrEqual(Getter<V> getter, V val) {
		return create(getter, Operator.LTE, val);
	}

	public static <V> Filter<V> create(Getter<V> getter, String operator, V val) {
		Objects.requireNonNull(operator, "empty operator");
		
		Operator fo = Operator.findByOperator(operator);
		
		if (fo == null) {
			throw new CasserMappingException("invalid operator " + operator);
		}
		
		return create(getter, fo, val);
	}
	
	public static <V> Filter<V> create(Getter<V> getter, Operator op, V val) {
		Objects.requireNonNull(getter, "empty getter");
		Objects.requireNonNull(op, "empty op");
		Objects.requireNonNull(val, "empty value");
		
		if (op == Operator.IN) {
			throw new IllegalArgumentException("invalid usage of the 'in' operator, use Filter.in() static method");
		}
		
		CasserMappingProperty prop = MappingUtil.resolveMappingProperty(getter);
		
		return new Filter<V>(prop, op, val);
	}
	
}
