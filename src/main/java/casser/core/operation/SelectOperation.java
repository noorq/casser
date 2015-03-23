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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import casser.core.AbstractSessionOperations;
import casser.core.Filter;
import casser.core.dsl.Getter;
import casser.mapping.CasserMappingEntity;
import casser.mapping.CasserMappingProperty;
import casser.mapping.ColumnValueProvider;
import casser.mapping.MappingUtil;
import casser.mapping.OrderingDirection;
import casser.mapping.RowColumnValueProvider;
import casser.support.CasserMappingException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Ordering;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.querybuilder.Select.Where;


public final class SelectOperation<E> extends AbstractFilterStreamOperation<E, SelectOperation<E>> {

	protected final Function<ColumnValueProvider, E> rowMapper;
	protected final CasserMappingProperty<?>[] props;
	
	protected List<Ordering> ordering = null;
	protected Integer limit = null;
	
	public SelectOperation(AbstractSessionOperations sessionOperations, Function<ColumnValueProvider, E> rowMapper, CasserMappingProperty<?>... props) {
		super(sessionOperations);
		this.rowMapper = rowMapper;
		this.props = props;	
	}
	
	public CountOperation count() {
		
		CasserMappingEntity<?> entity = null;
		for (CasserMappingProperty<?> prop : props) {
			
			if (entity == null) {
				entity = prop.getEntity();
			}
			else if (entity != prop.getEntity()) {
				throw new CasserMappingException("you can count records only from a single entity " + entity.getMappingInterface() + " or " + prop.getEntity().getMappingInterface());
			}
		}
		
		if (entity == null) {
			throw new CasserMappingException("no entity or table to count data");
		}
		
		return new CountOperation(sessionOperations, entity);
	}
	
	public <R> SelectTransformingOperation<R, E> map(Function<E, R> fn) {
		return new SelectTransformingOperation<R, E>(this, fn);
	}
	
	public SelectOperation<E> orderBy(Getter<?> getter, String direction) {
		Objects.requireNonNull(direction, "direction is null");
		return orderBy(getter, OrderingDirection.parseString(direction));
	}
	
	public SelectOperation<E> orderBy(Getter<?> getter, OrderingDirection direction) {
		Objects.requireNonNull(getter, "property is null");
		Objects.requireNonNull(direction, "direction is null");
		
		CasserMappingProperty<?> prop = MappingUtil.resolveMappingProperty(getter);
		
		if (!prop.isClusteringColumn()) {
			throw new CasserMappingException("property must be a clustering column " + prop.getPropertyName());
		}

		switch(direction) {
			case ASC:
				getOrCreateOrdering().add(QueryBuilder.asc(prop.getColumnName()));
				return this;
			case DESC:
				getOrCreateOrdering().add(QueryBuilder.desc(prop.getColumnName()));
				return this;
		}
		
		throw new CasserMappingException("unknown ordering direction " + direction);
	}

	public SelectOperation<E> limit(Integer limit) {
		this.limit = limit;
		return this;
	}
	
	@Override
	public BuiltStatement buildStatement() {
		
		CasserMappingEntity<?> entity = null;
		Selection selection = QueryBuilder.select();
		
		for (CasserMappingProperty<?> prop : props) {
			selection = selection.column(prop.getColumnName());
			
			if (entity == null) {
				entity = prop.getEntity();
			}
			else if (entity != prop.getEntity()) {
				throw new CasserMappingException("you can select columns only from a single entity " + entity.getMappingInterface() + " or " + prop.getEntity().getMappingInterface());
			}
		}
		
		if (entity == null) {
			throw new CasserMappingException("no entity or table to select data");
		}
		
		Select select = selection.from(entity.getTableName());
		
		if (ordering != null && !ordering.isEmpty()) {
			select.orderBy(ordering.toArray(new Ordering[ordering.size()]));
		}
		
		if (limit != null) {
			select.limit(limit.intValue());
		}
		
		if (filters != null && !filters.isEmpty()) {
		
			Where where = select.where();
			
			for (Filter<?> filter : filters) {
				where.and(filter.getClause());
			}
		}
		
		return select;
	}

	@Override
	public Stream<E> transform(ResultSet resultSet) {

		return StreamSupport.stream(
				Spliterators.spliteratorUnknownSize(resultSet.iterator(), Spliterator.ORDERED)
				, false).map(r -> new RowColumnValueProvider(r)).map(rowMapper);

	}


	private List<Ordering> getOrCreateOrdering() {
		if (ordering == null) {
			ordering = new ArrayList<Ordering>();
		}
		return ordering;
	}
}
