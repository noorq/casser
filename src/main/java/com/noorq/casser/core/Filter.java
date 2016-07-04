/*
 *      Copyright (C) 2015 The Casser Authors
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

import java.util.Objects;

import com.datastax.driver.core.querybuilder.Clause;
import com.noorq.casser.core.reflect.CasserPropertyNode;
import com.noorq.casser.mapping.MappingUtil;
import com.noorq.casser.mapping.value.ColumnValuePreparer;

public final class Filter<V> {

	private final CasserPropertyNode node;
	private final Postulate<V> postulate;
	
	private Filter(CasserPropertyNode node, Postulate<V> postulate) {
		this.node = node;
		this.postulate = postulate;
	}
	
	public CasserPropertyNode getNode() {
		return node;
	}

	public Clause getClause(ColumnValuePreparer valuePreparer) {
		return postulate.getClause(node, valuePreparer);
	}
	
	public static <V> Filter<V> equal(Getter<V> getter, V val) {
		return create(getter, Operator.EQ, val);
	}

	public static <V> Filter<V> in(Getter<V> getter, V... vals) {
		Objects.requireNonNull(getter, "empty getter");
		Objects.requireNonNull(vals, "empty values");
		
		if (vals.length == 0) {
			throw new IllegalArgumentException("values array is empty");
		}
		
		for (int i = 0; i != vals.length; ++i) {
			Objects.requireNonNull(vals[i], "value[" + i + "] is empty");
		}
		
		CasserPropertyNode node = MappingUtil.resolveMappingProperty(getter);
		
		Postulate<V> postulate = Postulate.of(Operator.IN, vals);
		
		return new Filter<V>(node, postulate);
	}
	
	public static <V> Filter<V> greaterThan(Getter<V> getter, V val) {
		return create(getter, Operator.GT, val);
	}
	
	public static <V> Filter<V> lessThan(Getter<V> getter, V val) {
		return create(getter, Operator.LT, val);
	}

	public static <V> Filter<V> greaterThanOrEqual(Getter<V> getter, V val) {
		return create(getter, Operator.GTE, val);
	}

	public static <V> Filter<V> lessThanOrEqual(Getter<V> getter, V val) {
		return create(getter, Operator.LTE, val);
	}

	public static <V> Filter<V> create(Getter<V> getter, Postulate<V> postulate) {
		Objects.requireNonNull(getter, "empty getter");
		Objects.requireNonNull(postulate, "empty operator");

		CasserPropertyNode node = MappingUtil.resolveMappingProperty(getter);
		
		return new Filter<V>(node, postulate);
	}
	
	public static <V> Filter<V> create(Getter<V> getter, Operator op, V val) {
		Objects.requireNonNull(getter, "empty getter");
		Objects.requireNonNull(op, "empty op");
		Objects.requireNonNull(val, "empty value");
		
		if (op == Operator.IN) {
			throw new IllegalArgumentException("invalid usage of the 'in' operator, use Filter.in() static method");
		}
		
		CasserPropertyNode node = MappingUtil.resolveMappingProperty(getter);
		
		Postulate<V> postulate = Postulate.of(op, val);
		
		return new Filter<V>(node, postulate);
	}

	@Override
	public String toString() {
		return node.getColumnName() + postulate.toString();
	}
	
	
	
}
