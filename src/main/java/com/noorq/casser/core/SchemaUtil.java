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
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.schemabuilder.Alter;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.Create.Options;
import com.datastax.driver.core.schemabuilder.CreateType;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import com.datastax.driver.core.schemabuilder.UDTType;
import com.noorq.casser.mapping.CasserEntityType;
import com.noorq.casser.mapping.CasserEntity;
import com.noorq.casser.mapping.CasserProperty;
import com.noorq.casser.mapping.IdentityName;
import com.noorq.casser.mapping.OrderingDirection;
import com.noorq.casser.support.CasserMappingException;
import com.noorq.casser.support.CqlUtil;
import com.noorq.casser.support.Either;

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
	
		if (entity.getType() != CasserEntityType.USER_DEFINED_TYPE) {
			throw new CasserMappingException("expected user defined type entity " + entity);
		}

		CreateType create = SchemaBuilder.createType(entity.getName().toCql());

		for (CasserProperty prop : entity.getProperties()) {
			
			if (prop.isPartitionKey() || prop.isClusteringColumn()) {
				throw new CasserMappingException("primary key columns are not supported in UserDefinedType for " + prop.getPropertyName() + " in entity " + entity);
			}
 			
			Either<DataType,IdentityName> type = prop.getDataType();
			
			if (type.isLeft()) {
				create.addColumn(prop.getColumnName().toCql(), type.getLeft());
			}
			else if (type.isRight()) {
				UDTType udtType = SchemaBuilder.frozen(type.getRight().toCql());
				create.addUDTColumn(prop.getColumnName().toCql(), udtType);
			}
			else {
				throwNoMapping(prop);
			}			
			
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
		
		List<CasserProperty> partitionKeys = new ArrayList<CasserProperty>();
		List<CasserProperty> clusteringColumns = new ArrayList<CasserProperty>();
		List<CasserProperty> columns = new ArrayList<CasserProperty>();

		for (CasserProperty prop : entity.getProperties()) {

			if (prop.isPartitionKey()) {
				partitionKeys.add(prop);
			} else if (prop.isClusteringColumn()) {
				clusteringColumns.add(prop);
			} else {
				columns.add(prop);
			}

		}

		Collections
				.sort(partitionKeys, OrdinalBasedPropertyComparator.INSTANCE);
		Collections.sort(clusteringColumns,
				OrdinalBasedPropertyComparator.INSTANCE);

		for (CasserProperty prop : partitionKeys) {
			
			Either<DataType,IdentityName> type = prop.getDataType();
			
			if (type.isRight()) {
				throw new CasserMappingException("user defined type can not be a partition key for " + prop.getPropertyName() + " in " + prop.getEntity());
			}
			
			create.addPartitionKey(prop.getColumnName().toCql(), type.getLeft());
		}

		for (CasserProperty prop : clusteringColumns) {
			
			Either<DataType,IdentityName> type = prop.getDataType();
			
			if (type.isLeft()) {
				create.addClusteringColumn(prop.getColumnName().toCql(), type.getLeft());
			}
			else if (type.isRight()) {
				UDTType udtType = SchemaBuilder.frozen(type.getRight().toCql());
				create.addUDTClusteringColumn(prop.getColumnName().toCql(), udtType);
			}
			else {
				throwNoMapping(prop);
			}
			
		}
		
		for (CasserProperty prop : columns) {
			
			Either<DataType,IdentityName> type = prop.getDataType();
			
			if (prop.isStatic()) {
				
				if (type.isLeft()) {
					create.addStaticColumn(prop.getColumnName().toCql(), type.getLeft());
				}
				else if (type.isRight()) {
					UDTType udtType = SchemaBuilder.frozen(type.getRight().toCql());
					create.addUDTStaticColumn(prop.getColumnName().toCql(), udtType);
				}
				else {
					throwNoMapping(prop);
				}
			}
			else {
				
				if (type.isLeft()) {
					create.addColumn(prop.getColumnName().toCql(), type.getLeft());
				}
				else if (type.isRight()) {
					UDTType udtType = SchemaBuilder.frozen(type.getRight().toCql());
					create.addUDTColumn(prop.getColumnName().toCql(), udtType);
				}
				else {
					throwNoMapping(prop);
				}
				
			}
		}

		if (!clusteringColumns.isEmpty()) {
			
			Options options = create.withOptions();
			
			for (CasserProperty prop : clusteringColumns) {
				options.clusteringOrder(prop.getColumnName().toCql(), mapDirection(prop.getOrdering()));
			}
			
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

		for (CasserProperty prop : entity.getProperties()) {

			String columnName = prop.getColumnName().getName();

			if (dropUnusedColumns) {
				visitedColumns.add(columnName);
			}

			if (prop.isPartitionKey() || prop.isClusteringColumn()) {
				throw new CasserMappingException(
						"unable to alter column that is a part of primary key '"
								+ columnName + "' for entity "
								+ entity);
			}
			
			Either<DataType,IdentityName> type = prop.getDataType();
			
			ColumnMetadata columnMetadata = tmd.getColumn(columnName);

			if (columnMetadata != null) {
				
				if  (type.isLeft()) {
					
					if (!type.getLeft().equals(columnMetadata.getType())) {
						result.add(alter.alterColumn(prop.getColumnName().toCql()).type(type.getLeft()));
					}
					
				}
				else if (type.isRight()) {
					
					DataType metadataType = columnMetadata.getType();
					if (metadataType.getName() == DataType.Name.UDT &&
							metadataType instanceof UserType) {
						
						UserType metadataUserType = (UserType) metadataType;
						
						if (!type.getRight().equals(metadataUserType.getTypeName())) {
							UDTType udtType = SchemaBuilder.frozen(type.getRight().toCql());
							result.add(alter.alterColumn(prop.getColumnName().toCql()).udtType(udtType));
						}
						
					}
					else {
						throw new CasserMappingException("expected UserType in metadata " + metadataType + " for " + prop.getPropertyName() + " in " + prop.getEntity());
					}
						
				}
				
			}
			else if (type.isLeft()) {
				result.add(alter.addColumn(prop.getColumnName().toCql()).type(type.getLeft()));
			}
			else if (type.isRight()) {
				UDTType udtType = SchemaBuilder.frozen(type.getRight().toCql());
				result.add(alter.addColumn(prop.getColumnName().toCql()).udtType(udtType));
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
		
		return entity.getProperties().stream()
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
		.getProperties()
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
