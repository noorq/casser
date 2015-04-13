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
package com.noorq.casser.mapping.type;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.schemabuilder.Alter;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.CreateType;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import com.noorq.casser.mapping.ColumnType;
import com.noorq.casser.mapping.IdentityName;
import com.noorq.casser.support.CasserMappingException;

public final class DTDataType extends AbstractDataType {

	private static final Class<?>[] EMPTY_CLASSES = new Class<?>[] {};
	
	private final DataType dataType;
	
	public DTDataType(ColumnType columnType, DataType dataType) {
		super(columnType);
		this.dataType = dataType;
	}

	public DataType getDataType() {
		return dataType;
	}

	@Override
	public Class<?>[] getUdtClasses() {
		return EMPTY_CLASSES;
	}
	
	@Override
	public void addColumn(Create create, IdentityName columnName) {
		
		switch(columnType) {
		
		case PARTITION_KEY:
			create.addPartitionKey(columnName.toCql(), dataType);
			break;
			
		case CLUSTERING_COLUMN:
			create.addClusteringColumn(columnName.toCql(), dataType);
			break;
			
		case STATIC_COLUMN:
			create.addStaticColumn(columnName.toCql(), dataType);
			break;
			
		case COLUMN:
			create.addColumn(columnName.toCql(), dataType);
			break;
		
		default:
			throwWrongColumnType(columnName);	
		}
		
	}

	@Override
	public void addColumn(CreateType create, IdentityName columnName) {
		
		if (columnType != ColumnType.COLUMN) {
			throwWrongColumnType(columnName);
		}
		
		create.addColumn(columnName.toCql(), dataType);
	}
	
	@Override
	public SchemaStatement alterColumn(Alter alter, IdentityName columnName, ColumnMetadata columnMetadata) {
		
		if (columnMetadata != null) {
			
			if (!dataType.equals(columnMetadata.getType())) {
				ensureSimpleColumn(columnName);
				
				return alter.alterColumn(columnName.toCql()).type(dataType);
			}
			
		}
		else {
			
			switch(columnType) {
			
			case STATIC_COLUMN:
				return alter.addStaticColumn(columnName.toCql()).type(dataType);
				
			case COLUMN:
				return alter.addColumn(columnName.toCql()).type(dataType);
				
			default:
				throw new CasserMappingException("unable to alter " + columnType + " column " + columnName);
			}
			
		}
		
		return null;
	}
	
}
