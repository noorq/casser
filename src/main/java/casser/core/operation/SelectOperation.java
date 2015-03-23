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

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import casser.core.AbstractSessionOperations;
import casser.core.Filter;
import casser.mapping.CasserMappingEntity;
import casser.mapping.CasserMappingProperty;
import casser.mapping.ColumnValueProvider;
import casser.mapping.RowColumnValueProvider;
import casser.support.CasserMappingException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.querybuilder.Select.Where;


public class SelectOperation<E> extends AbstractFilterStreamOperation<E, SelectOperation<E>> {

	private final Function<ColumnValueProvider, E> rowMapper;
	private final CasserMappingProperty<?>[] props;
	
	public SelectOperation(AbstractSessionOperations sessionOperations, Function<ColumnValueProvider, E> rowMapper, CasserMappingProperty<?>... props) {
		super(sessionOperations);
		this.rowMapper = rowMapper;
		this.props = props;	
	}
	
	public CountOperation count() {
		return null;
	}
	
	public <R> SelectOperation<R> map(Function<E, R> fn) {
		return null;
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
				throw new CasserMappingException("you can select columns only from single entity " + entity.getMappingInterface() + " or " + prop.getEntity().getMappingInterface());
			}
		}
		
		if (entity == null) {
			throw new CasserMappingException("no entity or table to select data");
		}
		
		Select select = selection.from(entity.getTableName());
		
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

	
}
