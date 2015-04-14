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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ColumnMetadata.IndexMetadata;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.schemabuilder.Alter;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.Create.Options;
import com.datastax.driver.core.schemabuilder.CreateType;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import com.noorq.casser.mapping.CasserEntity;
import com.noorq.casser.mapping.CasserEntityType;
import com.noorq.casser.mapping.CasserProperty;
import com.noorq.casser.mapping.ColumnType;
import com.noorq.casser.mapping.OrderingDirection;
import com.noorq.casser.support.CasserMappingException;
import com.noorq.casser.support.CqlUtil;

public final class SchemaUtil {

	private SchemaUtil() {
	}

	public static RegularStatement use(String keyspace, boolean forceQuote) {
		if (forceQuote) {
			return new SimpleStatement("USE" + CqlUtil.forceQuote(keyspace));
		}
		else {
			return new SimpleStatement("USE " + keyspace);
		}
	}

	public static SchemaStatement createUserType(CasserEntity entity) {
	
		if (entity.getType() != CasserEntityType.UDT) {
			throw new CasserMappingException("expected user defined type entity " + entity);
		}

		CreateType create = SchemaBuilder.createType(entity.getName().toCql());

		for (CasserProperty prop : entity.getOrderedProperties()) {
			
			ColumnType columnType = prop.getColumnType();

			if (columnType == ColumnType.PARTITION_KEY || columnType == ColumnType.CLUSTERING_COLUMN) {
				throw new CasserMappingException("primary key columns are not supported in UserDefinedType for " + prop.getPropertyName() + " in entity " + entity);
			}
 			
			prop.getDataType().addColumn(create, prop.getColumnName());
		}
		
		return create;
	}
	
	public static SchemaStatement dropUserType(CasserEntity entity) {
		return SchemaBuilder.dropType(entity.getName().toCql());
	}
	
	public static SchemaStatement createTable(CasserEntity entity) {
		
		if (entity.getType() != CasserEntityType.TABLE) {
			throw new CasserMappingException("expected table entity " + entity);
		}
		
		Create create = SchemaBuilder.createTable(entity.getName().toCql());

		create.ifNotExists();
				
		List<CasserProperty> clusteringColumns = new ArrayList<CasserProperty>();

		for (CasserProperty prop : entity.getOrderedProperties()) {

			ColumnType columnType = prop.getColumnType();

			if (columnType == ColumnType.CLUSTERING_COLUMN) {
				clusteringColumns.add(prop);
			}
			
			prop.getDataType().addColumn(create, prop.getColumnName());

		}

		if (!clusteringColumns.isEmpty()) {
			Options options = create.withOptions();
			clusteringColumns.forEach(p -> options.clusteringOrder(p.getColumnName().toCql(), mapDirection(p.getOrdering())));
		}
		
		return create;
		
	}

	public static List<SchemaStatement> alterTable(TableMetadata tmd,
			CasserEntity entity, boolean dropUnusedColumns) {

		if (entity.getType() != CasserEntityType.TABLE) {
			throw new CasserMappingException("expected table entity " + entity);
		}

		List<SchemaStatement> result = new ArrayList<SchemaStatement>();
		
		Alter alter = SchemaBuilder.alterTable(entity.getName().toCql());

		final Set<String> visitedColumns = dropUnusedColumns ? new HashSet<String>()
				: Collections.<String> emptySet();

		for (CasserProperty prop : entity.getOrderedProperties()) {

			String columnName = prop.getColumnName().getName();

			if (dropUnusedColumns) {
				visitedColumns.add(columnName);
			}
			
			ColumnType columnType = prop.getColumnType();

			if (columnType == ColumnType.PARTITION_KEY || columnType == ColumnType.CLUSTERING_COLUMN) {
				throw new CasserMappingException(
						"unable to alter column that is a part of primary key '"
								+ columnName + "' for entity "
								+ entity);
			}
			
			ColumnMetadata columnMetadata = tmd.getColumn(columnName);
			SchemaStatement stmt = prop.getDataType().alterColumn(alter, prop.getColumnName(), columnMetadata);
			
			if (stmt != null) {
				result.add(stmt);
			}
	
		}
		
		if (dropUnusedColumns) {
			for (ColumnMetadata cm : tmd.getColumns()) {
				if (!visitedColumns.contains(cm.getName())) {
			
					result.add(alter.dropColumn(cm.getName()));	
					
				}
			}
		}
		
		return result;
	}

	public static SchemaStatement dropTable(CasserEntity entity) {
		
		if (entity.getType() != CasserEntityType.TABLE) {
			throw new CasserMappingException("expected table entity " + entity);
		}

		return SchemaBuilder.dropTable(entity.getName().toCql()).ifExists();
		
	}
	
	public static SchemaStatement createIndex(CasserProperty prop) {
		
		return SchemaBuilder.createIndex(prop.getIndexName().get().toCql())
				.ifNotExists()
				.onTable(prop.getEntity().getName().toCql())
				.andColumn(prop.getColumnName().toCql());
		
	}

	public static List<SchemaStatement> createIndexes(CasserEntity entity) {
		
		return entity.getOrderedProperties().stream()
		.filter(p -> p.getIndexName().isPresent())
		.map(p -> SchemaUtil.createIndex(p))
		.collect(Collectors.toList());
		
	}
	
	public static List<SchemaStatement> alterIndexes(TableMetadata tmd,
			CasserEntity entity, boolean dropUnusedIndexes) {

		List<SchemaStatement> list = new ArrayList<SchemaStatement>();
		
		final Set<String> visitedColumns = dropUnusedIndexes ? new HashSet<String>()
				: Collections.<String> emptySet();
		
		entity
		.getOrderedProperties()
		.stream()
		.filter(p -> p.getIndexName().isPresent())
		.forEach(p -> {
			
			String columnName = p.getColumnName().getName();
			
			if (dropUnusedIndexes) {
				visitedColumns.add(columnName);
			}
			
			ColumnMetadata cm = tmd.getColumn(columnName);
			
			if (cm != null) {
				IndexMetadata im = cm.getIndex();
				if (im == null) {
					list.add(createIndex(p));
				}
			}
			else {
				list.add(createIndex(p));
			}
			
			
		});
		
		if (dropUnusedIndexes) {
			
			tmd
			.getColumns()
			.stream()
			.filter(c -> c.getIndex() != null && !visitedColumns.contains(c.getName()))
			.forEach(c -> {
				
				list.add(SchemaBuilder.dropIndex(c.getIndex().getName()).ifExists());
				
			});
			
			
		}
		
		return list;
		
	}
	
	public static SchemaStatement dropIndex(CasserProperty prop) {
		return SchemaBuilder.dropIndex(prop.getIndexName().get().toCql()).ifExists();
	}
	
	private static SchemaBuilder.Direction mapDirection(OrderingDirection o) {
		switch(o) {
		case ASC:
			return SchemaBuilder.Direction.ASC;
		case DESC:
			return SchemaBuilder.Direction.DESC;
		}
		throw new CasserMappingException("unknown ordering " + o);
	}
	
	public static void throwNoMapping(CasserProperty prop) {
		
		throw new CasserMappingException(
				"only primitive types and Set,List,Map collections and UserDefinedTypes are allowed, unknown type for property '" + prop.getPropertyName()
						+ "' type is '" + prop.getJavaType() + "' in the entity " + prop.getEntity());

	}
}
