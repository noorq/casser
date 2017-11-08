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
package net.helenus.core.operation;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;
import net.helenus.core.AbstractSessionOperations;
import net.helenus.core.Filter;
import net.helenus.core.reflect.HelenusPropertyNode;
import net.helenus.mapping.HelenusEntity;
import net.helenus.support.HelenusMappingException;

public final class CountOperation extends AbstractFilterOperation<Long, CountOperation> {

  private HelenusEntity entity;

  public CountOperation(AbstractSessionOperations sessionOperations) {
    super(sessionOperations);
  }

  public CountOperation(AbstractSessionOperations sessionOperations, HelenusEntity entity) {
    super(sessionOperations);
    this.entity = entity;
    //TODO(gburd): cache SELECT COUNT results within the scope of a UOW
  }

  @Override
  public BuiltStatement buildStatement(boolean cached) {

    if (filters != null && !filters.isEmpty()) {
      filters.forEach(f -> addPropertyNode(f.getNode()));
    }

    if (entity == null) {
      throw new HelenusMappingException("unknown entity");
    }

    Select select = QueryBuilder.select().countAll().from(entity.getName().toCql());

    if (filters != null && !filters.isEmpty()) {

      Where where = select.where();

      for (Filter<?> filter : filters) {
        where.and(filter.getClause(sessionOps.getValuePreparer()));
      }
    }

    return select;
  }

  @Override
  public Long transform(ResultSet resultSet) {
    return resultSet.one().getLong(0);
  }

  private void addPropertyNode(HelenusPropertyNode p) {
    if (entity == null) {
      entity = p.getEntity();
    } else if (entity != p.getEntity()) {
      throw new HelenusMappingException(
          "you can count columns only in single entity "
              + entity.getMappingInterface()
              + " or "
              + p.getEntity().getMappingInterface());
    }
  }
}
