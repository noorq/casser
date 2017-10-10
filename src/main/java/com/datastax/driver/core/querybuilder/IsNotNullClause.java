package com.datastax.driver.core.querybuilder;

import com.datastax.driver.core.CodecRegistry;
import java.util.List;

public class IsNotNullClause extends Clause {

  final String name;

  public IsNotNullClause(String name) {
    this.name = name;
  }

  @Override
  String name() {
    return name;
  }

  @Override
  Object firstValue() {
    return null;
  }

  @Override
  void appendTo(StringBuilder sb, List<Object> variables, CodecRegistry codecRegistry) {
    Utils.appendName(name, sb).append(" IS NOT NULL");
  }

  @Override
  boolean containsBindMarker() {
    return false;
  }
}
