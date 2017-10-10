package net.helenus.test.integration.core.views;

import java.util.Date;
import java.util.UUID;
import net.helenus.mapping.annotation.ClusteringColumn;
import net.helenus.mapping.annotation.Index;
import net.helenus.mapping.annotation.MaterializedView;
import net.helenus.mapping.annotation.PartitionKey;

@MaterializedView
public interface CyclistsByAge extends Cyclist {
  @PartitionKey
  UUID cid();

  @ClusteringColumn
  int age();

  Date birthday();

  @Index
  String country();
}
