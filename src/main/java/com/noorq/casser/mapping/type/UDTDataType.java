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
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.schemabuilder.Alter;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.CreateType;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import com.datastax.driver.core.schemabuilder.UDTType;
import com.noorq.casser.mapping.ColumnType;
import com.noorq.casser.mapping.IdentityName;

public final class UDTDataType extends AbstractDataType {

	private final IdentityName udtName;
	private final Class<?> udtClass;
	
	public UDTDataType(ColumnType columnType, IdentityName udtName, Class<?> udtClass) {
		super(columnType);
		this.udtName = udtName;
		this.udtClass = udtClass;
	}

	@Override
	public Class<?>[] getTypeArguments() {
		return new Class<?>[] { udtClass };
	}
	
	public IdentityName getUdtName() {
		return udtName;
	}

	@Override
	public void addColumn(Create create, IdentityName columnName) {
		
		UDTType udtType = SchemaBuilder.frozen(udtName.toCql());
		
		switch(columnType) {
		
		case PARTITION_KEY:
			create.addUDTPartitionKey(columnName.toCql(), udtType);
			break;

		case CLUSTERING_COLUMN:
			create.addUDTClusteringColumn(columnName.toCql(), udtType);
			break;
			
		case STATIC_COLUMN:
			create.addUDTStaticColumn(columnName.toCql(), udtType);
			break;
			
		case COLUMN:
			create.addUDTColumn(columnName.toCql(), udtType);
			break;
		
		default:
			throwWrongColumnType(columnName);	
			
		}
		
	}

	@Override
	public void addColumn(CreateType create, IdentityName columnName) {
		ensureSimpleColumn(columnName);
		
		UDTType udtType = SchemaBuilder.frozen(udtName.toCql());
		create.addUDTColumn(columnName.toCql(), udtType);
		
	}

	@Override
	public SchemaStatement alterColumn(Alter alter, IdentityName columnName,
			ColumnMetadata columnMetadata) {
		
		ensureSimpleColumn(columnName);
		
		if (columnMetadata != null) {
			
			DataType metadataType = columnMetadata.getType();
			if (metadataType.getName() == DataType.Name.UDT &&
					metadataType instanceof UserType) {
				
				UserType metadataUserType = (UserType) metadataType;
				
				if (!udtName.getName().equals(metadataUserType.getTypeName())) {
					
					UDTType udtType = SchemaBuilder.frozen(udtName.toCql());
					return alter.alterColumn(columnName.toCql()).udtType(udtType);
				}
				
			}
			else {
				
				UDTType udtType = SchemaBuilder.frozen(udtName.toCql());
				return alter.alterColumn(columnName.toCql()).udtType(udtType);
				
			}
			
		}
		else {
			
			UDTType udtType = SchemaBuilder.frozen(udtName.toCql());
			return alter.addColumn(columnName.toCql()).udtType(udtType);
			
		}

		return null;
	}

	@Override
	public String toString() {
		return "UDT<" + udtName + ">";
	}

}
