package net.helenus.test.integration.core.views;

import java.util.Date;
import java.util.UUID;
import net.helenus.mapping.annotation.ClusteringColumn;
import net.helenus.mapping.annotation.CoveringIndex;
import net.helenus.mapping.annotation.PartitionKey;
import net.helenus.mapping.annotation.Table;

@Table
@CoveringIndex(
  name = "cyclist_mv",
  covering = {"age", "birthday", "country"},
  partitionKeys = {"age", "cid"},
  clusteringColumns = {}
)
public interface Cyclist {
  @ClusteringColumn
  UUID cid();

  String name();

  @PartitionKey
  int age();

  Date birthday();

  String country();
}
