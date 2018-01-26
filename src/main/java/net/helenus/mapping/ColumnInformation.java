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
package net.helenus.mapping;

import java.lang.reflect.Method;
import net.helenus.mapping.annotation.ClusteringColumn;
import net.helenus.mapping.annotation.Column;
import net.helenus.mapping.annotation.PartitionKey;
import net.helenus.mapping.annotation.StaticColumn;
import net.helenus.support.HelenusMappingException;

public final class ColumnInformation {

  private final IdentityName columnName;
  private final ColumnType columnType;
  private final int ordinal;
  private final OrderingDirection ordering;

  public ColumnInformation(Method getter) {

    String columnName = null;
    boolean forceQuote = false;
    ColumnType columnTypeLocal = ColumnType.COLUMN;
    int ordinalLocal = 0;
    OrderingDirection orderingLocal = OrderingDirection.ASC;

    PartitionKey partitionKey = getter.getDeclaredAnnotation(PartitionKey.class);
    if (partitionKey != null) {
      columnName = partitionKey.value();
      forceQuote = partitionKey.forceQuote();
      columnTypeLocal = ColumnType.PARTITION_KEY;
      ordinalLocal = partitionKey.ordinal();
    }

    ClusteringColumn clusteringColumn = getter.getDeclaredAnnotation(ClusteringColumn.class);
    if (clusteringColumn != null) {
      ensureSingleColumnType(columnTypeLocal, getter);
      columnName = clusteringColumn.value();
      forceQuote = clusteringColumn.forceQuote();
      columnTypeLocal = ColumnType.CLUSTERING_COLUMN;
      ordinalLocal = clusteringColumn.ordinal();
      orderingLocal = clusteringColumn.ordering();
    }

    StaticColumn staticColumn = getter.getDeclaredAnnotation(StaticColumn.class);
    if (staticColumn != null) {
      ensureSingleColumnType(columnTypeLocal, getter);
      columnName = staticColumn.value();
      forceQuote = staticColumn.forceQuote();
      columnTypeLocal = ColumnType.STATIC_COLUMN;
      ordinalLocal = staticColumn.ordinal();
    }

    Column column = getter.getDeclaredAnnotation(Column.class);
    if (column != null) {
      ensureSingleColumnType(columnTypeLocal, getter);
      columnName = column.value();
      forceQuote = column.forceQuote();
      columnTypeLocal = ColumnType.COLUMN;
      ordinalLocal = column.ordinal();
    }

    if (columnName == null || columnName.isEmpty()) {
      columnName = MappingUtil.getDefaultColumnName(getter);
    }

    this.columnName = new IdentityName(columnName, forceQuote);
    this.columnType = columnTypeLocal;
    this.ordinal = ordinalLocal;
    this.ordering = orderingLocal;
  }

  public IdentityName getColumnName() {
    return columnName;
  }

  public ColumnType getColumnType() {
    return columnType;
  }

  public int getOrdinal() {
    return ordinal;
  }

  public OrderingDirection getOrdering() {
    return ordering;
  }

  private void ensureSingleColumnType(ColumnType columnTypeLocal, Method getter) {

    if (columnTypeLocal != ColumnType.COLUMN) {
      throw new HelenusMappingException(
          "property can be annotated only by a single column type " + getter);
    }
  }

  @Override
  public String toString() {
    return "ColumnInformation [columnName="
        + columnName
        + ", columnType="
        + columnType
        + ", ordinal="
        + ordinal
        + ", ordering="
        + ordering
        + "]";
  }
}
