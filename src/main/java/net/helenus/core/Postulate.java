/*
 *      Copyright (C) 2015 The Helenus Authors
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
package net.helenus.core;

import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import net.helenus.core.reflect.HelenusPropertyNode;
import net.helenus.mapping.value.ColumnValuePreparer;
import net.helenus.support.HelenusMappingException;

public final class Postulate<V> {

	private final Operator operator;
	private final V[] values;

	protected Postulate(Operator op, V[] values) {
		this.operator = op;
		this.values = values;
	}

	public static <V> Postulate<V> of(Operator op, V... values) {
		return new Postulate<V>(op, values);
	}

	public Clause getClause(HelenusPropertyNode node, ColumnValuePreparer valuePreparer) {

		switch (operator) {
			case EQ :
				return QueryBuilder.eq(node.getColumnName(),
						valuePreparer.prepareColumnValue(values[0], node.getProperty()));

			case IN :
				Object[] preparedValues = new Object[values.length];
				for (int i = 0; i != values.length; ++i) {
					preparedValues[i] = valuePreparer.prepareColumnValue(values[i], node.getProperty());
				}
				return QueryBuilder.in(node.getColumnName(), preparedValues);

			case LT :
				return QueryBuilder.lt(node.getColumnName(),
						valuePreparer.prepareColumnValue(values[0], node.getProperty()));

			case LTE :
				return QueryBuilder.lte(node.getColumnName(),
						valuePreparer.prepareColumnValue(values[0], node.getProperty()));

			case GT :
				return QueryBuilder.gt(node.getColumnName(),
						valuePreparer.prepareColumnValue(values[0], node.getProperty()));

			case GTE :
				return QueryBuilder.gte(node.getColumnName(),
						valuePreparer.prepareColumnValue(values[0], node.getProperty()));

			default :
				throw new HelenusMappingException("unknown filter operation " + operator);
		}
	}

	@Override
	public String toString() {

		if (operator == Operator.IN) {

			if (values == null) {
				return "in()";
			}

			int len = values.length;
			StringBuilder b = new StringBuilder();
			b.append("in(");
			for (int i = 0; i != len; i++) {
				if (b.length() > 3) {
					b.append(", ");
				}
				b.append(String.valueOf(values[i]));
			}
			return b.append(')').toString();
		}

		return operator.getName() + values[0];
	}
}
