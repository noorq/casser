/*
 *      Copyright (C) 2015 The Helenus Authors
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
package net.helenus.core;

import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import java.util.List;
import net.helenus.mapping.HelenusEntity;
import net.helenus.support.HelenusException;

public final class TableOperations {

  private final AbstractSessionOperations sessionOps;
  private final boolean dropUnusedColumns;
  private final boolean dropUnusedIndexes;

  public TableOperations(
      AbstractSessionOperations sessionOps, boolean dropUnusedColumns, boolean dropUnusedIndexes) {
    this.sessionOps = sessionOps;
    this.dropUnusedColumns = dropUnusedColumns;
    this.dropUnusedIndexes = dropUnusedIndexes;
  }

  public void createTable(HelenusEntity entity) {
    sessionOps.execute(SchemaUtil.createTable(entity));
    executeBatch(SchemaUtil.createIndexes(entity));
  }

  public void dropTable(HelenusEntity entity) {
    sessionOps.execute(SchemaUtil.dropTable(entity));
  }

  public void validateTable(TableMetadata tmd, HelenusEntity entity) {

    if (tmd == null) {
      throw new HelenusException(
          "table does not exists "
              + entity.getName()
              + "for entity "
              + entity.getMappingInterface());
    }

    List<SchemaStatement> list = SchemaUtil.alterTable(tmd, entity, dropUnusedColumns);

    list.addAll(SchemaUtil.alterIndexes(tmd, entity, dropUnusedIndexes));

    if (!list.isEmpty()) {
      throw new HelenusException(
          "schema changed for entity "
              + entity.getMappingInterface()
              + ", apply this command: "
              + list);
    }
  }

  public void updateTable(TableMetadata tmd, HelenusEntity entity) {
    if (tmd == null) {
      createTable(entity);
      return;
    }

    executeBatch(SchemaUtil.alterTable(tmd, entity, dropUnusedColumns));
    executeBatch(SchemaUtil.alterIndexes(tmd, entity, dropUnusedIndexes));
  }

  public void createView(HelenusEntity entity) {
    sessionOps.execute(
        SchemaUtil.createMaterializedView(
            sessionOps.usingKeyspace(), entity.getName().toCql(), entity));
    // executeBatch(SchemaUtil.createIndexes(entity)); NOTE: Unfortunately C* 3.10 does not yet support 2i on materialized views.
  }

  public void dropView(HelenusEntity entity) {
    sessionOps.execute(
        SchemaUtil.dropMaterializedView(
            sessionOps.usingKeyspace(), entity.getName().toCql(), entity));
  }

  public void updateView(TableMetadata tmd, HelenusEntity entity) {
    if (tmd == null) {
      createTable(entity);
      return;
    }

    executeBatch(SchemaUtil.alterTable(tmd, entity, dropUnusedColumns));
    executeBatch(SchemaUtil.alterIndexes(tmd, entity, dropUnusedIndexes));
  }

  private void executeBatch(List<SchemaStatement> list) {

    list.forEach(s -> sessionOps.execute(s));
  }
}
