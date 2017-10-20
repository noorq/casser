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

import java.util.*;
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
import com.google.common.collect.Iterables;

import net.helenus.core.*;
import net.helenus.core.cache.EntityIdentifyingFacet;
import net.helenus.core.cache.Facet;
import net.helenus.core.reflect.HelenusPropertyNode;
import net.helenus.mapping.HelenusEntity;
import net.helenus.mapping.MappingUtil;
import net.helenus.mapping.OrderingDirection;
import net.helenus.mapping.value.ColumnValueProvider;
import net.helenus.mapping.value.ValueProviderMap;
import net.helenus.support.Fun;
import net.helenus.support.HelenusMappingException;

public final class SelectOperation<E> extends AbstractFilterStreamOperation<E, SelectOperation<E>> {

	protected final List<HelenusPropertyNode> props = new ArrayList<HelenusPropertyNode>();
	protected Function<Row, E> rowMapper = null;
	protected List<Ordering> ordering = null;
	protected Integer limit = null;
	protected boolean allowFiltering = false;
	protected String alternateTableName = null;

	@SuppressWarnings("unchecked")
	public SelectOperation(AbstractSessionOperations sessionOperations) {
		super(sessionOperations);

		this.rowMapper = new Function<Row, E>() {

			@Override
			public E apply(Row source) {

				ColumnValueProvider valueProvider = sessionOps.getValueProvider();
				Object[] arr = new Object[props.size()];

				int i = 0;
				for (HelenusPropertyNode p : props) {
					Object value = valueProvider.getColumnValue(source, -1, p.getProperty());
					arr[i++] = value;
				}

				return (E) Fun.ArrayTuple.of(arr);
			}
		};
	}

	public SelectOperation(AbstractSessionOperations sessionOperations, HelenusEntity entity) {

		super(sessionOperations);

		entity.getOrderedProperties().stream().map(p -> new HelenusPropertyNode(p, Optional.empty()))
				.forEach(p -> this.props.add(p));
	}

	public SelectOperation(AbstractSessionOperations sessionOperations, HelenusEntity entity,
			Function<Row, E> rowMapper) {

		super(sessionOperations);
		this.rowMapper = rowMapper;

		entity.getOrderedProperties().stream().map(p -> new HelenusPropertyNode(p, Optional.empty()))
				.forEach(p -> this.props.add(p));
	}

	public SelectOperation(AbstractSessionOperations sessionOperations, Function<Row, E> rowMapper,
			HelenusPropertyNode... props) {

		super(sessionOperations);

		this.rowMapper = rowMapper;
		Collections.addAll(this.props, props);
	}

	public CountOperation count() {

		HelenusEntity entity = null;
		for (HelenusPropertyNode prop : props) {

			if (entity == null) {
				entity = prop.getEntity();
			} else if (entity != prop.getEntity()) {
				throw new HelenusMappingException("you can count records only from a single entity "
						+ entity.getMappingInterface() + " or " + prop.getEntity().getMappingInterface());
			}
		}

		return new CountOperation(sessionOps, entity);
	}

	public <V extends E> SelectOperation<E> from(Class<V> materializedViewClass) {
		Objects.requireNonNull(materializedViewClass);
		HelenusEntity entity = Helenus.entity(materializedViewClass);
		this.alternateTableName = entity.getName().toCql();
		this.props.clear();
		entity.getOrderedProperties().stream().map(p -> new HelenusPropertyNode(p, Optional.empty()))
				.forEach(p -> this.props.add(p));
		return this;
	}

	public SelectFirstOperation<E> single() {
		limit(1);
		return new SelectFirstOperation<E>(this);
	}

	public <R> SelectTransformingOperation<R, E> mapTo(Class<R> entityClass) {

		Objects.requireNonNull(entityClass, "entityClass is null");

		HelenusEntity entity = Helenus.entity(entityClass);

		this.rowMapper = null;

		return new SelectTransformingOperation<R, E>(this, (r) -> {
			Map<String, Object> map = new ValueProviderMap(r, sessionOps.getValueProvider(), entity);
			return (R) Helenus.map(entityClass, map);
		});
	}

	public <R> SelectTransformingOperation<R, E> map(Function<E, R> fn) {
		return new SelectTransformingOperation<R, E>(this, fn);
	}

	public SelectOperation<E> column(Getter<?> getter) {
		HelenusPropertyNode p = MappingUtil.resolveMappingProperty(getter);
		this.props.add(p);
		return this;
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

	public SelectOperation<E> allowFiltering() {
		this.allowFiltering = true;
		return this;
	}

	@Override
	public Map<String, EntityIdentifyingFacet> getIdentifyingFacets() {
		HelenusEntity entity = props.get(0).getEntity();
		return entity.getIdentifyingFacets();
	}

	@Override
	public Set<Facet> bindFacetValues() {
		HelenusEntity entity = props.get(0).getEntity();
		Set<Facet> boundFacets = new HashSet<Facet>();
		// Check to see if this select statement has enough information to build one or
		// more identifying facets.
		entity.getIdentifyingFacets().forEach((facetName, facet) -> {
			EntityIdentifyingFacet.Binder binder = facet.binder();
			facet.getProperties().forEach(prop -> {
				Filter filter = filters.get(prop);
				if (filter != null) {
					binder.setValueForProperty(prop, filter.toString());
				} else if (facetName.equals("*")) {
					binder.setValueForProperty(prop, "");
				}
			});
			if (binder.isFullyBound()) {
				boundFacets.add(binder.bind());
			}
		});
		return boundFacets;
	}

	@Override
	public BuiltStatement buildStatement(boolean cached) {

		HelenusEntity entity = null;
		Selection selection = QueryBuilder.select();

		for (HelenusPropertyNode prop : props) {
			String columnName = prop.getColumnName();
			selection = selection.column(columnName);

			if (prop.getProperty().caseSensitiveIndex()) {
				allowFiltering = true;
			}

			if (entity == null) {
				entity = prop.getEntity();
			} else if (entity != prop.getEntity()) {
				throw new HelenusMappingException("you can select columns only from a single entity "
						+ entity.getMappingInterface() + " or " + prop.getEntity().getMappingInterface());
			}

			if (cached) {
				switch (prop.getProperty().getColumnType()) {
					case PARTITION_KEY :
					case CLUSTERING_COLUMN :
						break;
					default :
						if (entity.equals(prop.getEntity())) {
							if (prop.getNext().isPresent()) {
								columnName = Iterables.getLast(prop).getColumnName().toCql(true);
							}
							if (!prop.getProperty().getDataType().isCollectionType()) {
								selection.writeTime(columnName).as(columnName + "_writeTime");
								selection.ttl(columnName).as(columnName + "_ttl");
							}
						}
						break;
				}
			}
		}

		if (entity == null) {
			throw new HelenusMappingException("no entity or table to select data");
		}

		String tableName = alternateTableName == null ? entity.getName().toCql() : alternateTableName;
		Select select = selection.from(tableName);

		if (ordering != null && !ordering.isEmpty()) {
			select.orderBy(ordering.toArray(new Ordering[ordering.size()]));
		}

		if (limit != null) {
			select.limit(limit);
		}

		if (filters != null && !filters.isEmpty()) {

			Where where = select.where();

			for (Filter<?> filter : filters.values()) {
				where.and(filter.getClause(sessionOps.getValuePreparer()));
			}
		}

		if (ifFilters != null && !ifFilters.isEmpty()) {
			logger.error("onlyIf conditions " + ifFilters + " would be ignored in the statement " + select);
		}

		if (allowFiltering) {
			select.allowFiltering();
		}

		return select;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Stream<E> transform(ResultSet resultSet) {
		if (rowMapper != null) {
			return StreamSupport
					.stream(Spliterators.spliteratorUnknownSize(resultSet.iterator(), Spliterator.ORDERED), false)
					.map(rowMapper);
		} else {
			return (Stream<E>) StreamSupport
					.stream(Spliterators.spliteratorUnknownSize(resultSet.iterator(), Spliterator.ORDERED), false);
		}
	}

	private List<Ordering> getOrCreateOrdering() {
		if (ordering == null) {
			ordering = new ArrayList<Ordering>();
		}
		return ordering;
	}
}
