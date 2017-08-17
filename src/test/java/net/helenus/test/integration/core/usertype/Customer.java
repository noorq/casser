package net.helenus.test.integration.core.usertype;

import java.util.UUID;
import net.helenus.mapping.annotation.Column;
import net.helenus.mapping.annotation.PartitionKey;
import net.helenus.mapping.annotation.Table;

@Table
public interface Customer {

  @PartitionKey
  UUID id();

  @Column
  AddressInformation addressInformation();
}
