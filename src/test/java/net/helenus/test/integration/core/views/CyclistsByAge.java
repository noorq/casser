package net.helenus.test.integration.core.views;

import net.helenus.mapping.annotation.ClusteringColumn;
import net.helenus.mapping.annotation.Index;
import net.helenus.mapping.annotation.MaterializedView;
import net.helenus.mapping.annotation.PartitionKey;

import java.util.Date;
import java.util.UUID;

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
