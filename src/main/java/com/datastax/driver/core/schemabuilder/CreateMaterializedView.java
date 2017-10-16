package com.datastax.driver.core.schemabuilder;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.querybuilder.Select;

public class CreateMaterializedView extends Create {

  private String viewName;
  private Select.Where selection;
  private String primaryKey;
  private String clustering;

  public CreateMaterializedView(
      String keyspaceName,
      String viewName,
      Select.Where selection,
      String primaryKey,
      String clustering) {
    super(keyspaceName, viewName);
    this.viewName = viewName;
    this.selection = selection;
    this.primaryKey = primaryKey;
    this.clustering = clustering;
  }

  public String getQueryString(CodecRegistry codecRegistry) {
    return buildInternal();
  }

  public String buildInternal() {
    StringBuilder createStatement =
        new StringBuilder(STATEMENT_START).append("CREATE MATERIALIZED VIEW");
    if (ifNotExists) {
      createStatement.append(" IF NOT EXISTS");
    }
    createStatement.append(" ");
    if (keyspaceName.isPresent()) {
      createStatement.append(keyspaceName.get()).append(".");
    }
    createStatement.append(viewName);
    createStatement.append(" AS ");
    createStatement.append(selection.getQueryString());
    createStatement.setLength(createStatement.length() - 1);
    createStatement.append(" ");
    createStatement.append(primaryKey);
    if (clustering != null) {
      createStatement.append(" ").append(clustering);
    }
    createStatement.append(";");

    return createStatement.toString();
  }

  public String toString() {
    return buildInternal();
  }
}
