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

import casser.mapping.CasserMappingEntity;
import casser.mapping.CasserMappingProperty;
import casser.support.CasserMappingException;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.schemabuilder.Alter;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;

public final class SchemaUtil {

	private SchemaUtil() {
	}

	public static String useCql(String keyspace) {
		return "USE " + keyspace;
	}
	
	public static String createTableCql(CasserMappingEntity<?> entity) {

		Create create = SchemaBuilder.createTable(entity.getTableName());

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
			create.addClusteringColumn(prop.getColumnName(), prop.getDataType());
		}

		for (CasserMappingProperty<?> prop : columns) {
			
			if (prop.isStatic()) {
				create.addStaticColumn(prop.getColumnName(), prop.getDataType());
			}
			else {
				create.addColumn(prop.getColumnName(), prop.getDataType());
			}
		}

		return create.buildInternal();
		
	}

	public static String alterTableCql(TableMetadata tmd,
			CasserMappingEntity<?> entity, boolean dropRemovedColumns) {

		boolean altered = false;
		
		Alter alter = SchemaBuilder.alterTable(entity.getTableName());

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
								+ entity.getName());
			}

			if (columnMetadata == null) {

				alter.addColumn(columnName).type(columnDataType);
				altered = true;
				
			} else {

				alter.alterColumn(columnName).type(columnDataType);
				altered = true;
				
			}
		}

		if (altered) {
			return alter.buildInternal();
		}
		
		return null;
	}

	public static String dropTableCql(CasserMappingEntity<?> entity) {
		
		return SchemaBuilder.dropTable(entity.getTableName()).buildInternal();
		
	}
	
}
