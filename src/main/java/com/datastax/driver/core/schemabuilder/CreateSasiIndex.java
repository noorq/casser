package com.datastax.driver.core.schemabuilder;

public class CreateSasiIndex extends CreateCustomIndex {

  public CreateSasiIndex(String indexName) {
    super(indexName);
  }

  String getCustomClassName() {
    return "org.apache.cassandra.index.sasi.SASIIndex";
  }

  String getOptions() {
    return "'analyzer_class': "
        + "'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer', "
        + "'case_sensitive': 'false'";
  }
}
