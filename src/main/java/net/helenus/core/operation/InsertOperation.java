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
package net.helenus.core.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import net.helenus.core.AbstractSessionOperations;
import net.helenus.core.Getter;
import net.helenus.core.reflect.HelenusPropertyNode;
import net.helenus.mapping.HelenusEntity;
import net.helenus.mapping.HelenusProperty;
import net.helenus.mapping.MappingUtil;
import net.helenus.mapping.value.BeanColumnValueProvider;
import net.helenus.support.Fun;
import net.helenus.support.HelenusMappingException;

public final class InsertOperation extends AbstractOperation<ResultSet, InsertOperation> {

	private HelenusEntity entity;

	private final List<Fun.Tuple2<HelenusPropertyNode, Object>> values = new ArrayList<Fun.Tuple2<HelenusPropertyNode, Object>>();
	private boolean ifNotExists;

	private int[] ttl;
	private long[] timestamp;

	public InsertOperation(AbstractSessionOperations sessionOperations, boolean ifNotExists) {
		super(sessionOperations);

		this.ifNotExists = ifNotExists;
	}

	public InsertOperation(AbstractSessionOperations sessionOperations, HelenusEntity entity, Object pojo,
			boolean ifNotExists) {
		super(sessionOperations);

		this.ifNotExists = ifNotExists;

		for (HelenusProperty prop : entity.getOrderedProperties()) {

			Object value = BeanColumnValueProvider.INSTANCE.getColumnValue(pojo, -1, prop);

			value = sessionOps.getValuePreparer().prepareColumnValue(value, prop);

			if (value != null) {

				HelenusPropertyNode node = new HelenusPropertyNode(prop, Optional.empty());
				values.add(Fun.Tuple2.of(node, value));
			}

		}

	}

	public InsertOperation ifNotExists() {
		this.ifNotExists = true;
		return this;
	}

	public InsertOperation ifNotExists(boolean enable) {
		this.ifNotExists = enable;
		return this;
	}

	public <V> InsertOperation value(Getter<V> getter, V val) {

		Objects.requireNonNull(getter, "getter is empty");

		if (val != null) {
			HelenusPropertyNode node = MappingUtil.resolveMappingProperty(getter);

			Object value = sessionOps.getValuePreparer().prepareColumnValue(val, node.getProperty());

			if (value != null) {
				values.add(Fun.Tuple2.of(node, value));
			}
		}

		return this;
	}

	@Override
	public BuiltStatement buildStatement() {

		values.forEach(t -> addPropertyNode(t._1));

		if (entity == null) {
			throw new HelenusMappingException("unknown entity");
		}

		Insert insert = QueryBuilder.insertInto(entity.getName().toCql());

		if (ifNotExists) {
			insert.ifNotExists();
		}

		values.forEach(t -> {
			insert.value(t._1.getColumnName(), t._2);
		});

		if (this.ttl != null) {
			insert.using(QueryBuilder.ttl(this.ttl[0]));
		}
		if (this.timestamp != null) {
			insert.using(QueryBuilder.timestamp(this.timestamp[0]));
		}

		return insert;
	}

	@Override
	public ResultSet transform(ResultSet resultSet) {
		return resultSet;
	}

	public InsertOperation usingTtl(int ttl) {
		this.ttl = new int[1];
		this.ttl[0] = ttl;
		return this;
	}

	public InsertOperation usingTimestamp(long timestamp) {
		this.timestamp = new long[1];
		this.timestamp[0] = timestamp;
		return this;
	}

	private void addPropertyNode(HelenusPropertyNode p) {
		if (entity == null) {
			entity = p.getEntity();
		} else if (entity != p.getEntity()) {
			throw new HelenusMappingException("you can insert only single entity " + entity.getMappingInterface()
					+ " or " + p.getEntity().getMappingInterface());
		}
	}
}