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

import java.util.List;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import com.noorq.casser.mapping.CasserMappingEntity;
import com.noorq.casser.support.CasserException;

public final class TableOperations {

	private final AbstractSessionOperations sessionOps;
	private final boolean dropUnusedColumns;
	private final boolean dropUnusedIndexes;
	
	public TableOperations(AbstractSessionOperations sessionOps, boolean dropUnusedColumns, boolean dropUnusedIndexes) {
		this.sessionOps = sessionOps;
		this.dropUnusedColumns = dropUnusedColumns;
		this.dropUnusedIndexes = dropUnusedIndexes;
	}
	
	public void createTable(CasserMappingEntity entity) {
		
		sessionOps.execute(SchemaUtil.createTable(entity));
		
		executeBatch(SchemaUtil.createIndexes(entity));
		
	}
	
	public void validateTable(TableMetadata tmd, CasserMappingEntity entity) {
		
		if (tmd == null) {
			throw new CasserException("table not exists " + entity.getName() + "for entity " + entity.getMappingInterface());
		}
		
		List<SchemaStatement> list = SchemaUtil.alterTable(tmd, entity, dropUnusedColumns);
		
		list.addAll(SchemaUtil.alterIndexes(tmd, entity, dropUnusedIndexes));
		
		if (!list.isEmpty()) {
			throw new CasserException("schema changed for entity " + entity.getMappingInterface() + ", apply this command: " + list);
		}
	}
	
	public void updateTable(TableMetadata tmd, CasserMappingEntity entity) {
		
		if (tmd == null) {
			createTable(entity);
			return;
		}
		
		executeBatch(SchemaUtil.alterTable(tmd, entity, dropUnusedColumns));
		executeBatch(SchemaUtil.alterIndexes(tmd, entity, dropUnusedIndexes));
	}
	
	private void executeBatch(List<SchemaStatement> list) {
		if (!list.isEmpty()) {
			Batch b = QueryBuilder.batch(list.toArray(new RegularStatement[list.size()]));
			sessionOps.execute(b);
		}
	}
	
}
