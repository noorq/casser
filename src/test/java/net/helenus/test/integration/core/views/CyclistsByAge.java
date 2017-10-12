package net.helenus.test.integration.core.views;

import java.util.Date;
import java.util.UUID;

import net.helenus.mapping.OrderingDirection;
import net.helenus.mapping.annotation.*;

@MaterializedView
public interface CyclistsByAge extends Cyclist {
  @PartitionKey
  UUID cid();

  @ClusteringColumn(ordering = OrderingDirection.ASC)
  int age();

  Date birthday();

  @Index
  String country();
}
