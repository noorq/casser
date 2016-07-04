/*
 *      Copyright (C) 2015 The Casser Authors
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

import com.datastax.driver.core.schemabuilder.Alter;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.CreateType;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import com.noorq.casser.mapping.ColumnType;
import com.noorq.casser.mapping.IdentityName;
import com.noorq.casser.support.CasserMappingException;

public abstract class AbstractDataType {

	public abstract void addColumn(Create create, IdentityName columnName);

	public abstract void addColumn(CreateType create, IdentityName columnName);

	public abstract SchemaStatement alterColumn(Alter alter, IdentityName columnName, OptionalColumnMetadata columnInformation);
	
	public abstract Class<?>[] getTypeArguments();
	
	final ColumnType columnType;
	
	public AbstractDataType(ColumnType columnType) {
		this.columnType = columnType;
	}

	public ColumnType getColumnType() {
		return columnType;
	}

	void ensureSimpleColumn(IdentityName columnName) {
		if (columnType != ColumnType.COLUMN) {
			throwWrongColumnType(columnName);
		}
	}
	
	void throwWrongColumnType(IdentityName columnName) {
		throw new CasserMappingException("wrong column type " + columnType + " for UserDefinedType in columnName " + columnName);
	}
	
}
