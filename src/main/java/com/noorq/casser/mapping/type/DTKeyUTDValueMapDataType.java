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
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import com.datastax.driver.core.schemabuilder.UDTType;
import com.noorq.casser.mapping.ColumnType;
import com.noorq.casser.mapping.IdentityName;
import com.noorq.casser.support.CasserMappingException;

public final class DTKeyUTDValueMapDataType extends AbstractDataType {

	private final DataType keyType;
	private final IdentityName valueType;
	private final Class<?> udtValueClass;
	
	public DTKeyUTDValueMapDataType(ColumnType columnType, DataType keyType, IdentityName valueType, Class<?> udtValueClass) {
		super(columnType);
		this.keyType = keyType;
		this.valueType = valueType;
		this.udtValueClass = udtValueClass;
	}

	@Override
	public Class<?>[] getUdtClasses() {
		return new Class<?>[] { udtValueClass };
	}
	
	@Override
	public void addColumn(Create create, IdentityName columnName) {
		ensureSimpleColumn(columnName);

		UDTType valueUdtType = SchemaBuilder.frozen(valueType.toCql());
		create.addUDTMapColumn(columnName.toCql(), keyType, valueUdtType);
	}

	@Override
	public void addColumn(CreateType create, IdentityName columnName) {
		ensureSimpleColumn(columnName);

		UDTType valueUdtType = SchemaBuilder.frozen(valueType.toCql());
		create.addUDTMapColumn(columnName.toCql(), keyType, valueUdtType);
	}

	@Override
	public SchemaStatement alterColumn(Alter alter, IdentityName columnName,
			ColumnMetadata columnMetadata) {
		throw new CasserMappingException("alter of UDTMap column is not possible now for " + columnName);
	}
	
}
