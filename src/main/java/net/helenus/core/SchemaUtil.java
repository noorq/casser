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

import com.datastax.driver.core.*;
import com.datastax.driver.core.IndexMetadata;
import com.datastax.driver.core.querybuilder.IsNotNullClause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.schemabuilder.*;
import com.datastax.driver.core.schemabuilder.Create.Options;
import java.util.*;
import java.util.stream.Collectors;
import net.helenus.core.reflect.HelenusPropertyNode;
import net.helenus.mapping.*;
import net.helenus.mapping.ColumnType;
import net.helenus.mapping.annotation.ClusteringColumn;
import net.helenus.mapping.type.OptionalColumnMetadata;
import net.helenus.support.CqlUtil;
import net.helenus.support.HelenusMappingException;

public final class SchemaUtil {

  private SchemaUtil() {}

  public static RegularStatement use(String keyspace, boolean forceQuote) {
    if (forceQuote) {
      return new SimpleStatement("USE" + CqlUtil.forceQuote(keyspace));
    } else {
      return new SimpleStatement("USE " + keyspace);
    }
  }

  public static SchemaStatement createUserType(HelenusEntity entity) {

    if (entity.getType() != HelenusEntityType.UDT) {
      throw new HelenusMappingException("expected UDT entity " + entity);
    }

    CreateType create = SchemaBuilder.createType(entity.getName().toCql());

    for (HelenusProperty prop : entity.getOrderedProperties()) {

      ColumnType columnType = prop.getColumnType();

      if (columnType == ColumnType.PARTITION_KEY || columnType == ColumnType.CLUSTERING_COLUMN) {
        throw new HelenusMappingException(
            "primary key columns are not supported in UserDefinedType for "
                + prop.getPropertyName()
                + " in entity "
                + entity);
      }

      try {
        prop.getDataType().addColumn(create, prop.getColumnName());
      } catch (IllegalArgumentException e) {
        throw new HelenusMappingException(
            "invalid column name '"
                + prop.getColumnName()
                + "' in entity '"
                + entity.getName().getName()
                + "'",
            e);
      }
    }

    return create;
  }

  public static List<SchemaStatement> alterUserType(
      UserType userType, HelenusEntity entity, boolean dropUnusedColumns) {

    if (entity.getType() != HelenusEntityType.UDT) {
      throw new HelenusMappingException("expected UDT entity " + entity);
    }

    List<SchemaStatement> result = new ArrayList<SchemaStatement>();

    /**
     * TODO: In future replace SchemaBuilder.alterTable by SchemaBuilder.alterType when it will
     * exist
     */
    Alter alter = SchemaBuilder.alterTable(entity.getName().toCql());

    final Set<String> visitedColumns =
        dropUnusedColumns ? new HashSet<String>() : Collections.<String>emptySet();

    for (HelenusProperty prop : entity.getOrderedProperties()) {

      String columnName = prop.getColumnName().getName();

      if (dropUnusedColumns) {
        visitedColumns.add(columnName);
      }

      ColumnType columnType = prop.getColumnType();

      if (columnType == ColumnType.PARTITION_KEY || columnType == ColumnType.CLUSTERING_COLUMN) {
        continue;
      }

      DataType dataType = userType.getFieldType(columnName);
      SchemaStatement stmt =
          prop.getDataType()
              .alterColumn(alter, prop.getColumnName(), optional(columnName, dataType));

      if (stmt != null) {
        result.add(stmt);
      }
    }

    if (dropUnusedColumns) {
      for (String field : userType.getFieldNames()) {
        if (!visitedColumns.contains(field)) {

          result.add(alter.dropColumn(field));
        }
      }
    }

    return result;
  }

  public static SchemaStatement dropUserType(HelenusEntity entity) {

    if (entity.getType() != HelenusEntityType.UDT) {
      throw new HelenusMappingException("expected UDT entity " + entity);
    }

    return SchemaBuilder.dropType(entity.getName().toCql()).ifExists();
  }

  public static SchemaStatement dropUserType(UserType type) {

    return SchemaBuilder.dropType(type.getTypeName()).ifExists();
  }

  public static SchemaStatement createMaterializedView(
      String keyspace, String viewName, HelenusEntity entity) {
    if (entity.getType() != HelenusEntityType.VIEW) {
      throw new HelenusMappingException("expected view entity " + entity);
    }

    if (entity == null) {
      throw new HelenusMappingException("no entity or table to select data");
    }

    List<HelenusPropertyNode> props = new ArrayList<HelenusPropertyNode>();
    entity
        .getOrderedProperties()
        .stream()
        .map(p -> new HelenusPropertyNode(p, Optional.empty()))
        .forEach(p -> props.add(p));

    Select.Selection selection = QueryBuilder.select();

    for (HelenusPropertyNode prop : props) {
      String columnName = prop.getColumnName();
      selection = selection.column(columnName);
    }
    Class<?> iface = entity.getMappingInterface();
    String tableName = Helenus.entity(iface.getInterfaces()[0]).getName().toCql();
    Select.Where where = selection.from(tableName).where();
    List<String> p = new ArrayList<String>(props.size());
    List<String> c = new ArrayList<String>(props.size());
    List<String> o = new ArrayList<String>(props.size());

    for (HelenusPropertyNode prop : props) {
      String columnName = prop.getColumnName();
      switch (prop.getProperty().getColumnType()) {
        case PARTITION_KEY:
          p.add(columnName);
          where = where.and(new IsNotNullClause(columnName));
          break;

        case CLUSTERING_COLUMN:
          c.add(columnName);
          where = where.and(new IsNotNullClause(columnName));

          ClusteringColumn clusteringColumn =
              prop.getProperty().getGetterMethod().getAnnotation(ClusteringColumn.class);
          if (clusteringColumn != null && clusteringColumn.ordering() != null) {
            o.add(columnName + " " + clusteringColumn.ordering().cql());
          }
          break;
        default:
          break;
      }
    }

    String primaryKey =
        "PRIMARY KEY ("
            + ((p.size() > 1) ? "(" + String.join(", ", p) + ")" : p.get(0))
            + ((c.size() > 0)
                ? ", " + ((c.size() > 1) ? "(" + String.join(", ", c) + ")" : c.get(0))
                : "")
            + ")";

    String clustering = "";
    if (o.size() > 0) {
      clustering = "WITH CLUSTERING ORDER BY (" + String.join(", ", o) + ")";
    }
    return new CreateMaterializedView(keyspace, viewName, where, primaryKey, clustering)
        .ifNotExists();
  }

  public static SchemaStatement dropMaterializedView(
      String keyspace, String viewName, HelenusEntity entity) {
    return new DropMaterializedView(keyspace, viewName);
  }

  public static SchemaStatement createTable(HelenusEntity entity) {

    if (entity.getType() != HelenusEntityType.TABLE) {
      throw new HelenusMappingException("expected table entity " + entity);
    }

    // NOTE: There is a bug in the normal path of createTable where the
    // "cache" is set too early and never unset preventing more than
    // one column on a table.
    // SchemaBuilder.createTable(entity.getName().toCql());
    CreateTable create = new CreateTable(entity.getName().toCql());

    create.ifNotExists();

    List<HelenusProperty> clusteringColumns = new ArrayList<HelenusProperty>();

    for (HelenusProperty prop : entity.getOrderedProperties()) {

      ColumnType columnType = prop.getColumnType();

      if (columnType == ColumnType.CLUSTERING_COLUMN) {
        clusteringColumns.add(prop);
      }

      prop.getDataType().addColumn(create, prop.getColumnName());
    }

    if (!clusteringColumns.isEmpty()) {
      Options options = create.withOptions();
      clusteringColumns.forEach(
          p -> options.clusteringOrder(p.getColumnName().toCql(), mapDirection(p.getOrdering())));
    }

    return create;
  }

  public static List<SchemaStatement> alterTable(
      TableMetadata tmd, HelenusEntity entity, boolean dropUnusedColumns) {

    if (entity.getType() != HelenusEntityType.TABLE) {
      throw new HelenusMappingException("expected table entity " + entity);
    }

    List<SchemaStatement> result = new ArrayList<SchemaStatement>();

    Alter alter = SchemaBuilder.alterTable(entity.getName().toCql());

    final Set<String> visitedColumns =
        dropUnusedColumns ? new HashSet<String>() : Collections.<String>emptySet();

    for (HelenusProperty prop : entity.getOrderedProperties()) {

      String columnName = prop.getColumnName().getName();

      if (dropUnusedColumns) {
        visitedColumns.add(columnName);
      }

      ColumnType columnType = prop.getColumnType();

      if (columnType == ColumnType.PARTITION_KEY || columnType == ColumnType.CLUSTERING_COLUMN) {
        continue;
      }

      ColumnMetadata columnMetadata = tmd.getColumn(columnName);
      SchemaStatement stmt =
          prop.getDataType().alterColumn(alter, prop.getColumnName(), optional(columnMetadata));

      if (stmt != null) {
        result.add(stmt);
      }
    }

    if (dropUnusedColumns) {
      for (ColumnMetadata cm : tmd.getColumns()) {
        if (!visitedColumns.contains(cm.getName())) {

          result.add(alter.dropColumn(cm.getName()));
        }
      }
    }

    return result;
  }

  public static SchemaStatement dropTable(HelenusEntity entity) {

    if (entity.getType() != HelenusEntityType.TABLE) {
      throw new HelenusMappingException("expected table entity " + entity);
    }

    return SchemaBuilder.dropTable(entity.getName().toCql()).ifExists();
  }

  public static SchemaStatement createIndex(HelenusProperty prop) {
    if (prop.caseSensitiveIndex()) {
      return SchemaBuilder.createIndex(prop.getIndexName().get().toCql())
          .ifNotExists()
          .onTable(prop.getEntity().getName().toCql())
          .andColumn(prop.getColumnName().toCql());
    } else {
      return new CreateSasiIndex(prop.getIndexName().get().toCql())
          .ifNotExists()
          .onTable(prop.getEntity().getName().toCql())
          .andColumn(prop.getColumnName().toCql());
    }
  }

  public static List<SchemaStatement> createIndexes(HelenusEntity entity) {

    return entity
        .getOrderedProperties()
        .stream()
        .filter(p -> p.getIndexName().isPresent())
        .map(p -> SchemaUtil.createIndex(p))
        .collect(Collectors.toList());
  }

  public static List<SchemaStatement> alterIndexes(
      TableMetadata tmd, HelenusEntity entity, boolean dropUnusedIndexes) {

    List<SchemaStatement> list = new ArrayList<SchemaStatement>();

    final Set<String> visitedColumns =
        dropUnusedIndexes ? new HashSet<String>() : Collections.<String>emptySet();

    entity
        .getOrderedProperties()
        .stream()
        .filter(p -> p.getIndexName().isPresent())
        .forEach(
            p -> {
              String columnName = p.getColumnName().getName();

              if (dropUnusedIndexes) {
                visitedColumns.add(columnName);
              }

              ColumnMetadata cm = tmd.getColumn(columnName);

              if (cm != null) {
                IndexMetadata im = tmd.getIndex(columnName);
                if (im == null) {
                  list.add(createIndex(p));
                }
              } else {
                list.add(createIndex(p));
              }
            });

    if (dropUnusedIndexes) {

      tmd.getColumns()
          .stream()
          .filter(c -> tmd.getIndex(c.getName()) != null && !visitedColumns.contains(c.getName()))
          .forEach(
              c -> {
                list.add(SchemaBuilder.dropIndex(tmd.getIndex(c.getName()).getName()).ifExists());
              });
    }

    return list;
  }

  public static SchemaStatement dropIndex(HelenusProperty prop) {
    return SchemaBuilder.dropIndex(prop.getIndexName().get().toCql()).ifExists();
  }

  private static SchemaBuilder.Direction mapDirection(OrderingDirection o) {
    switch (o) {
      case ASC:
        return SchemaBuilder.Direction.ASC;
      case DESC:
        return SchemaBuilder.Direction.DESC;
    }
    throw new HelenusMappingException("unknown ordering " + o);
  }

  public static void throwNoMapping(HelenusProperty prop) {

    throw new HelenusMappingException(
        "only primitive types and Set,List,Map collections and UserDefinedTypes are allowed, unknown type for property '"
            + prop.getPropertyName()
            + "' type is '"
            + prop.getJavaType()
            + "' in the entity "
            + prop.getEntity());
  }

  private static OptionalColumnMetadata optional(final ColumnMetadata columnMetadata) {
    if (columnMetadata != null) {
      return new OptionalColumnMetadata() {

        @Override
        public String getName() {
          return columnMetadata.getName();
        }

        @Override
        public DataType getType() {
          return columnMetadata.getType();
        }
      };
    }
    return null;
  }

  private static OptionalColumnMetadata optional(final String name, final DataType dataType) {
    if (dataType != null) {
      return new OptionalColumnMetadata() {

        @Override
        public String getName() {
          return name;
        }

        @Override
        public DataType getType() {
          return dataType;
        }
      };
    }
    return null;
  }
}
