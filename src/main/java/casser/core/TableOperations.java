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

import casser.mapping.CasserMappingEntity;
import casser.support.CasserException;

import com.datastax.driver.core.TableMetadata;

public final class TableOperations {

	private final AbstractSessionOperations sessionOps;
	private final boolean dropRemovedColumns;
	
	public TableOperations(AbstractSessionOperations sessionOps, boolean dropRemovedColumns) {
		this.sessionOps = sessionOps;
		this.dropRemovedColumns = dropRemovedColumns;
	}
	
	public void createTable(CasserMappingEntity<?> entity) {
		
		String cql = SchemaUtil.createTableCql(entity);
		
		sessionOps.execute(cql);
		
	}
	
	public void validateTable(TableMetadata tmd, CasserMappingEntity<?> entity) {
		
		if (tmd == null) {
			throw new CasserException("table not exists " + entity.getTableName() + "for entity " + entity.getMappingInterface());
		}
		
		String cql = SchemaUtil.alterTableCql(tmd, entity, dropRemovedColumns);
		
		if (cql != null) {
			throw new CasserException("schema changed for entity " + entity.getMappingInterface() + ", apply this command: " + cql);
		}
	}
	
	public void updateTable(TableMetadata tmd, CasserMappingEntity<?> entity) {
		
		if (tmd == null) {
			createTable(entity);
		}
		
		String cql = SchemaUtil.alterTableCql(tmd, entity, dropRemovedColumns);
		
		if (cql != null) {
			sessionOps.execute(cql);
		}
	}
	
}
