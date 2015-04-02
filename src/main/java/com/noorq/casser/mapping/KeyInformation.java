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

public final class KeyInformation {

	private final boolean isPartitionKey;
	private final boolean isClusteringColumn;
	private final int ordinal;
	private final OrderingDirection ordering;
	
	public KeyInformation(Method getter) {

		boolean isPartitionKeyLocal = false;
		boolean isClusteringColumnLocal = false;
		int ordinalLocal = 0;
		OrderingDirection orderingLocal = OrderingDirection.ASC;
		
		PartitionKey partitionKey = getter.getDeclaredAnnotation(PartitionKey.class);
		
		if (partitionKey != null) {
			isPartitionKeyLocal = true;
			ordinalLocal = partitionKey.ordinal();
		}
		
		ClusteringColumn clusteringColumnn = getter.getDeclaredAnnotation(ClusteringColumn.class);
		
		if (clusteringColumnn != null) {
			
			if (isPartitionKeyLocal) {
				throw new CasserMappingException("property can be annotated only by single column type " + getter);
			}
			
			isClusteringColumnLocal = true;
			ordinalLocal = clusteringColumnn.ordinal();
			orderingLocal = clusteringColumnn.ordering();
		}
		
		this.isPartitionKey = isPartitionKeyLocal;
		this.isClusteringColumn = isClusteringColumnLocal;
		this.ordinal = ordinalLocal;
		this.ordering = orderingLocal;
	}

	public boolean isPartitionKey() {
		return isPartitionKey;
	}

	public boolean isClusteringColumn() {
		return isClusteringColumn;
	}

	public int getOrdinal() {
		return ordinal;
	}

	public OrderingDirection getOrdering() {
		return ordering;
	}
	
	
	
}
