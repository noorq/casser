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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Ordering;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.noorq.casser.core.AbstractSessionOperations;
import com.noorq.casser.core.Casser;
import com.noorq.casser.core.Filter;
import com.noorq.casser.core.Getter;
import com.noorq.casser.core.Ordered;
import com.noorq.casser.core.reflect.CasserPropertyNode;
import com.noorq.casser.mapping.CasserEntity;
import com.noorq.casser.mapping.MappingUtil;
import com.noorq.casser.mapping.OrderingDirection;
import com.noorq.casser.mapping.value.ColumnValueProvider;
import com.noorq.casser.mapping.value.ValueProviderMap;
import com.noorq.casser.support.CasserMappingException;
import com.noorq.casser.support.Fun.ArrayTuple;


public final class SelectOperation<E> extends AbstractFilterStreamOperation<E, SelectOperation<E>> {

	protected final ColumnValueProvider valueProvider;
	protected Function<Row, E> rowMapper = null;
	protected final List<CasserPropertyNode> props = new ArrayList<CasserPropertyNode>();
	
	protected List<Ordering> ordering = null;
	protected Integer limit = null;
	
	public SelectOperation(AbstractSessionOperations sessionOperations, 
			ColumnValueProvider valueProvider) {
		super(sessionOperations);
		this.valueProvider = valueProvider;
		this.rowMapper = new Function<Row, E>() {

			@Override
			public E apply(Row source) {
				
				Object[] arr = new Object[props.size()];
				
				int i = 0;
				for (CasserPropertyNode p : props) {
					Object value = valueProvider.getColumnValue(source, -1, p.getProperty());
					arr[i++] = value;
				}
				
				return (E) ArrayTuple.of(arr);
			}
			
		};
	}
	
	public SelectOperation(AbstractSessionOperations sessionOperations, 
			CasserEntity entity, 
			ColumnValueProvider valueProvider) {
		
		super(sessionOperations);
		this.valueProvider = valueProvider;
		
		entity.getOrderedProperties()
		.stream()
		.map(p -> new CasserPropertyNode(p, Optional.empty()))
		.forEach(p -> this.props.add(p));
		
	}
	
	public SelectOperation(AbstractSessionOperations sessionOperations, 
			CasserEntity entity, 
			ColumnValueProvider valueProvider,
			Function<Row, E> rowMapper) {
		
		super(sessionOperations);
		this.valueProvider = valueProvider;
		this.rowMapper = rowMapper;
		
		entity.getOrderedProperties()
		.stream()
		.map(p -> new CasserPropertyNode(p, Optional.empty()))
		.forEach(p -> this.props.add(p));
		
	}

	public SelectOperation(AbstractSessionOperations sessionOperations, 
			ColumnValueProvider valueProvider, 
			Function<Row, E> rowMapper, 
			CasserPropertyNode... props) {
		
		super(sessionOperations);
		this.valueProvider = valueProvider;
		this.rowMapper = rowMapper;
		Collections.addAll(this.props, props);
	}
	
	public CountOperation count() {
		
		CasserEntity entity = null;
		for (CasserPropertyNode prop : props) {
			
			if (entity == null) {
				entity = prop.getEntity();
			}
			else if (entity != prop.getEntity()) {
				throw new CasserMappingException("you can count records only from a single entity " + entity.getMappingInterface() + " or " + prop.getEntity().getMappingInterface());
			}
		}
		
		return new CountOperation(sessionOps, entity);
	}
	
	public <R> SelectTransformingOperation<R, E> mapTo(Class<R> entityClass) {
		
		Objects.requireNonNull(entityClass, "entityClass is null");
		
		if (this.valueProvider == null) {
			throw new CasserMappingException("mapTo operator is not available in current configuration");
		}
		
		CasserEntity entity = Casser.entity(entityClass);
		
		this.rowMapper = null;
		
		return new SelectTransformingOperation<R, E>(this, (r) -> {

			Map<String, Object> map = new ValueProviderMap(r, valueProvider, entity);
			return (R) Casser.map(entityClass, map);
			
		});
	}
	
	public SelectOperation<E> column(Getter<?> getter) {
		CasserPropertyNode p = MappingUtil.resolveMappingProperty(getter);
		this.props.add(p);
		return this;
	}
	
	public <R> SelectTransformingOperation<R, E> map(Function<E, R> fn) {
		return new SelectTransformingOperation<R, E>(this, fn);
	}
	
	public SelectOperation<E> orderBy(Getter<?> getter, OrderingDirection direction) {
		getOrCreateOrdering().add(new Ordered(getter, direction).getOrdering());
		return this;
	}
	
	public SelectOperation<E> orderBy(Ordered ordered) {
		getOrCreateOrdering().add(ordered.getOrdering());
		return this;
	}

	public SelectOperation<E> limit(Integer limit) {
		this.limit = limit;
		return this;
	}
	
	@Override
	public BuiltStatement buildStatement() {
		
		CasserEntity entity = null;
		Selection selection = QueryBuilder.select();
		
		for (CasserPropertyNode prop : props) {
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
		
		Select select = selection.from(entity.getName().toCql());
		
		if (ordering != null && !ordering.isEmpty()) {
			select.orderBy(ordering.toArray(new Ordering[ordering.size()]));
		}
		
		if (limit != null) {
			select.limit(limit.intValue());
		}

		if (filters != null && !filters.isEmpty()) {
		
			Where where = select.where();
			
			for (Filter<?> filter : filters) {
				where.and(filter.getClause(sessionOps.getValuePreparer()));
			}
		}
		
		if (ifFilters != null && !ifFilters.isEmpty()) {
			logger.warn("onlyIf conditions " + ifFilters + " will be ignored in the statement " + select);
		}
		
		return select;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Stream<E> transform(ResultSet resultSet) {
		
		if (rowMapper != null) {
		
			return StreamSupport.stream(
					Spliterators.spliteratorUnknownSize(resultSet.iterator(), Spliterator.ORDERED)
					, false).map(rowMapper);
		}
		
		else {
		
			return (Stream<E>) StreamSupport.stream(
					Spliterators.spliteratorUnknownSize(resultSet.iterator(), Spliterator.ORDERED)
					, false);

		}
	}


	private List<Ordering> getOrCreateOrdering() {
		if (ordering == null) {
			ordering = new ArrayList<Ordering>();
		}
		return ordering;
	}
	
}
