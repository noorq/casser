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
package com.noorq.casser.mapping;

import java.lang.reflect.Method;

import com.noorq.casser.support.CasserMappingException;

public final class ColumnInformation {

	private final IdentityName columnName;
	private final ColumnType columnType;
	private final int ordinal;
	private final OrderingDirection ordering;
	
	public ColumnInformation(Method getter) {

		String columnName = null;
		boolean forceQuote = false;
		ColumnType columnTypeLocal = ColumnType.COLUMN;
		int ordinalLocal = 0;
		OrderingDirection orderingLocal = OrderingDirection.ASC;
		
		PartitionKey partitionKey = getter.getDeclaredAnnotation(PartitionKey.class);
		
		if (partitionKey != null) {
			columnName = partitionKey.value();
			forceQuote = partitionKey.forceQuote();
			columnTypeLocal = ColumnType.PARTITION_KEY;
			ordinalLocal = partitionKey.ordinal();
		}
		
		ClusteringColumn clusteringColumn = getter.getDeclaredAnnotation(ClusteringColumn.class);
		
		if (clusteringColumn != null) {
			ensureSingleColumnType(columnTypeLocal, getter);
			columnName = clusteringColumn.value();
			forceQuote = clusteringColumn.forceQuote();
			columnTypeLocal = ColumnType.CLUSTERING_COLUMN;
			ordinalLocal = clusteringColumn.ordinal();
			orderingLocal = clusteringColumn.ordering();
		}
		
		Column column = getter.getDeclaredAnnotation(Column.class);
		if (column != null) {
			ensureSingleColumnType(columnTypeLocal, getter);
			columnName = column.value();
			forceQuote = column.forceQuote();
			columnTypeLocal = column.isStatic() ? ColumnType.STATIC_COLUMN : ColumnType.COLUMN;
		}
		
		if (columnName == null || columnName.isEmpty()) {
			columnName = MappingUtil.getDefaultColumnName(getter);
		}
		
		this.columnName = new IdentityName(columnName, forceQuote);
		this.columnType = columnTypeLocal;
		this.ordinal = ordinalLocal;
		this.ordering = orderingLocal;
	}
	
	public IdentityName getColumnName() {
		return columnName;
	}

	public ColumnType getColumnType() {
		return columnType;
	}

	public int getOrdinal() {
		return ordinal;
	}

	public OrderingDirection getOrdering() {
		return ordering;
	}
	
	private void ensureSingleColumnType(ColumnType columnTypeLocal, Method getter) {
		
		if (columnTypeLocal != ColumnType.COLUMN) {
			throw new CasserMappingException("property can be annotated only by a single column type " + getter);
		}
		
	}
	
}
