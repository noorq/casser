package com.datastax.driver.core.schemabuilder;

import com.google.common.base.Optional;

public class DropMaterializedView extends Drop {

  private Optional<String> keyspaceName = Optional.absent();
  private String itemName;
  private boolean ifExists = true;

  public DropMaterializedView(String keyspaceName, String viewName) {
    this(keyspaceName, viewName, DroppedItem.MATERIALIZED_VIEW);
  }

  private DropMaterializedView(String keyspaceName, String viewName, DroppedItem itemType) {
    super(keyspaceName, viewName, Drop.DroppedItem.TABLE);
    validateNotEmpty(keyspaceName, "Keyspace name");
    this.keyspaceName = Optional.fromNullable(keyspaceName);
    this.itemName = viewName;
  }

  /**
   * Add the 'IF EXISTS' condition to this DROP statement.
   *
   * @return this statement.
   */
  public Drop ifExists() {
    this.ifExists = true;
    return this;
  }

  @Override
  public String buildInternal() {
    StringBuilder dropStatement = new StringBuilder("DROP MATERIALIZED VIEW ");
    if (ifExists) {
      dropStatement.append("IF EXISTS ");
    }
    if (keyspaceName.isPresent()) {
      dropStatement.append(keyspaceName.get()).append(".");
    }

    dropStatement.append(itemName);
    return dropStatement.toString();
  }

  enum DroppedItem {
    TABLE,
    TYPE,
    INDEX,
    MATERIALIZED_VIEW
  }
}
