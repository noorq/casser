package net.helenus.test.integration.core.draft;

import java.util.Map;
import java.util.UUID;
import net.helenus.core.AbstractAuditedEntityDraft;
import net.helenus.core.Helenus;
import net.helenus.core.reflect.MapExportable;
import net.helenus.mapping.annotation.*;

@Table
public interface Inventory {

  static Inventory inventory = Helenus.dsl(Inventory.class);

  @Transient
  static Draft draft(UUID id) {
    return new Draft(id);
  }

  @PartitionKey
  UUID id();

  @Column("emea")
  @Types.Counter
  long EMEA();

  @Column("noram")
  @Types.Counter
  long NORAM();

  @Column("apac")
  @Types.Counter
  long APAC();

  @Transient
  default Draft update() {
    return new Draft(this);
  }

  class Draft extends AbstractAuditedEntityDraft<Inventory> {

    // Entity/Draft pattern-enabling methods:
    Draft(UUID id) {
      super(null);

      // Primary Key:
      set(inventory::id, id);
    }

    Draft(Inventory inventory) {
      super((MapExportable) inventory);
    }

    public Class<Inventory> getEntityClass() {
      return Inventory.class;
    }

    protected String getCurrentAuditor() {
      return "unknown";
    }

    // Immutable properties:
    public UUID id() {
      return this.<UUID>get(inventory::id, UUID.class);
    }

    public long EMEA() {
      return this.<Long>get(inventory::EMEA, long.class);
    }

    public Draft EMEA(long count) {
      mutate(inventory::EMEA, count);
      return this;
    }

    public long APAC() {
      return this.<Long>get(inventory::APAC, long.class);
    }

    public Draft APAC(long count) {
      mutate(inventory::APAC, count);
      return this;
    }

    public long NORAM() {
      return this.<Long>get(inventory::NORAM, long.class);
    }

    public Draft NORAM(long count) {
      mutate(inventory::NORAM, count);
      return this;
    }

  }
}
