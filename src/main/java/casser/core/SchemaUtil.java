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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import casser.mapping.CasserEntityType;
import casser.mapping.CasserMappingEntity;
import casser.mapping.CasserMappingProperty;
import casser.mapping.CqlUtil;
import casser.mapping.OrderingDirection;
import casser.support.CasserMappingException;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.schemabuilder.Alter;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.Create.Options;
import com.datastax.driver.core.schemabuilder.CreateType;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaStatement;

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

	public static SchemaStatement createUserType(CasserMappingEntity<?> entity) {
	
		if (entity.getType() != CasserEntityType.USER_DEFINED_TYPE) {
			throw new CasserMappingException("expected user defined type entity " + entity);
		}

		CreateType create = SchemaBuilder.createType(entity.getName());

		for (CasserMappingProperty<?> prop : entity.getMappingProperties()) {
			
			String columnName = prop.getColumnName();
			
			if (prop.isPartitionKey() || prop.isClusteringColumn()) {
				throw new CasserMappingException("primary key columns are not supported in UserDefinedType for column " + columnName + " in entity " + entity);
			}
 			
			create.addColumn(columnName, prop.getDataType());
			
		}
		
		return create;
	}
	
	public static SchemaStatement createTable(CasserMappingEntity<?> entity) {
		
		if (entity.getType() != CasserEntityType.TABLE) {
			throw new CasserMappingException("expected table entity " + entity);
		}
		
		Create create = SchemaBuilder.createTable(entity.getName());

		List<CasserMappingProperty<?>> partitionKeys = new ArrayList<CasserMappingProperty<?>>();
		List<CasserMappingProperty<?>> clusteringColumns = new ArrayList<CasserMappingProperty<?>>();
		List<CasserMappingProperty<?>> columns = new ArrayList<CasserMappingProperty<?>>();

		for (CasserMappingProperty<?> prop : entity.getMappingProperties()) {

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

		for (CasserMappingProperty<?> prop : partitionKeys) {
			create.addPartitionKey(prop.getColumnName(), prop.getDataType());
		}

		for (CasserMappingProperty<?> prop : clusteringColumns) {
			
			if (prop.getDataType() != null) {
				create.addClusteringColumn(prop.getColumnName(), prop.getDataType());
			}
			else if (prop.getUDTType() != null) {
				create.addUDTClusteringColumn(prop.getColumnName(), prop.getUDTType());
			}
			else {
				throwNoMapping(prop);
			}
			
		}
		
		for (CasserMappingProperty<?> prop : columns) {
			
			if (prop.isStatic()) {
				
				if (prop.getDataType() != null) {
					create.addStaticColumn(prop.getColumnName(), prop.getDataType());
				}
				else if (prop.getUDTType() != null) {
					create.addUDTStaticColumn(prop.getColumnName(), prop.getUDTType());
				}
				else {
					throwNoMapping(prop);
				}
			}
			else {
				
				if (prop.getDataType() != null) {
					create.addColumn(prop.getColumnName(), prop.getDataType());
				}
				else if (prop.getUDTType() != null) {
					create.addUDTColumn(prop.getColumnName(), prop.getUDTType());
				}
				else {
					throwNoMapping(prop);
				}
				
			}
		}

		if (!clusteringColumns.isEmpty()) {
			
			Options options = create.withOptions();
			
			for (CasserMappingProperty<?> prop : clusteringColumns) {
				options.clusteringOrder(prop.getColumnName(), mapDirection(prop.getOrdering()));
			}
			
		}
		
		return create;
		
	}

	public static List<SchemaStatement> alterTable(TableMetadata tmd,
			CasserMappingEntity<?> entity, boolean dropRemovedColumns) {

		if (entity.getType() != CasserEntityType.TABLE) {
			throw new CasserMappingException("expected table entity " + entity);
		}

		List<SchemaStatement> result = new ArrayList<SchemaStatement>();
		
		Alter alter = SchemaBuilder.alterTable(entity.getName());

		final Set<String> visitedColumns = dropRemovedColumns ? new HashSet<String>()
				: Collections.<String> emptySet();

		for (CasserMappingProperty<?> prop : entity.getMappingProperties()) {

			String columnName = prop.getColumnName();
			DataType columnDataType = prop.getDataType();

			String loweredColumnName = columnName.toLowerCase();

			if (dropRemovedColumns) {
				visitedColumns.add(loweredColumnName);
			}

			ColumnMetadata columnMetadata = tmd.getColumn(loweredColumnName);

			if (columnMetadata != null
					&& columnDataType.equals(columnMetadata.getType())) {
				continue;
			}

			if (prop.isPartitionKey() || prop.isClusteringColumn()) {
				throw new CasserMappingException(
						"unable to alter column that is a part of primary key '"
								+ columnName + "' for entity "
								+ entity);
			}

			if (columnMetadata == null) {

				result.add(alter.addColumn(columnName).type(columnDataType));
				
			} else {

				result.add(alter.alterColumn(columnName).type(columnDataType));
				
			}
		}
		
		if (dropRemovedColumns) {
			for (ColumnMetadata cm : tmd.getColumns()) {
				if (!visitedColumns.contains(cm.getName())) {
			
					result.add(alter.dropColumn(cm.getName()));	
					
				}
			}
		}
		
		return result;
	}

	public static SchemaStatement dropTable(CasserMappingEntity<?> entity) {
		
		if (entity.getType() != CasserEntityType.TABLE) {
			throw new CasserMappingException("expected table entity " + entity);
		}

		return SchemaBuilder.dropTable(entity.getName());
		
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
	
	public static void throwNoMapping(CasserMappingProperty<?> prop) {
		
		throw new CasserMappingException(
				"only primitive types and Set,List,Map collections and UserDefinedTypes are allowed, unknown type for property '" + prop.getPropertyName()
						+ "' type is '" + prop.getJavaType() + "' in the entity " + prop.getEntity());

	}
}
