package net.helenus.test.integration.core.draft;

import com.datastax.driver.core.utils.UUIDs;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import net.helenus.core.AbstractEntityDraft;
import net.helenus.core.Helenus;
import net.helenus.core.reflect.MapExportable;
import net.helenus.mapping.annotation.*;

@Table
public interface Supply {

  static Supply supply = Helenus.dsl(Supply.class);

  @PartitionKey
  UUID id();

  @ClusteringColumn(ordinal = 0)
  default String region() {
    return "NORAM";
  }

  @Index(caseSensitive = false)
  String code();

  @Index
  String description(); // @IndexText == lucene index

  @Index
  Map<String, Long> demand();

  @Index
  List<String> suppliers();

  @Index
  Set<String> shipments();

  @Transient
  static Draft draft(String region) {
    return new Draft(region);
  }

  @Transient
  default Draft update() {
    return new Draft(this);
  }

  class Draft extends AbstractEntityDraft<Supply> {

    // Entity/Draft pattern-enabling methods:
    Draft(String region) {
      super(null);

      // Primary Key:
      set(supply::id, UUIDs.timeBased());
      set(supply::region, region);
    }

    Draft(Supply supply) {
      super((MapExportable) supply);
    }

    public Class<Supply> getEntityClass() {
      return Supply.class;
    }

    // Immutable properties:
    public UUID id() {
      return this.<UUID>get(supply::id, UUID.class);
    }

    public String region() {
      return this.<String>get(supply::region, String.class);
    }

    // Mutable properties:
    public String code() {
      return this.<String>get(supply::code, String.class);
    }

    public Draft code(String code) {
      mutate(supply::code, code);
      return this;
    }

    public Draft setCode(String code) {
      return code(code);
    }

    public String description() {
      return this.<String>get(supply::description, String.class);
    }

    public Draft description(String description) {
      mutate(supply::description, description);
      return this;
    }

    public Draft setDescription(String description) {
      return description(description);
    }

    public Map<String, Long> demand() {
      return this.<Map<String, Long>>get(supply::demand, Map.class);
    }

    public Draft demand(Map<String, Long> demand) {
      mutate(supply::demand, demand);
      return this;
    }

    public Draft setDemand(Map<String, Long> demand) {
      return demand(demand);
    }

    public List<String> suppliers() {
      return this.<List<String>>get(supply::suppliers, List.class);
    }

    public Draft suppliers(List<String> suppliers) {
      mutate(supply::suppliers, suppliers);
      return this;
    }

    public Draft setSuppliers(List<String> suppliers) {
      return suppliers(suppliers);
    }

    public Set<String> shipments() {
      return this.<Set<String>>get(supply::shipments, Set.class);
    }

    public Draft shipments(Set<String> shipments) {
      mutate(supply::shipments, shipments);
      return this;
    }

    public Draft setshipments(Set<String> shipments) {
      return shipments(shipments);
    }
  }
}
