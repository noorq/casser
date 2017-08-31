package net.helenus.test.integration.core.draft;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;

import net.helenus.core.AbstractEntityDraft;
import net.helenus.core.reflect.MapExportable;
import net.helenus.mapping.annotation.*;


@Table
public interface Supply {

    @PartitionKey UUID id();
    @ClusteringColumn(ordinal=0) default String region() { return "NORAM"; }

    @Index(caseSensitive = false) String code();
    @Index String description(); // @IndexText == lucene index
    @Index Map<String, Long> demand();
    @Index List<String> suppliers();
    @Index Set<String> shipments();

    @Transient static Draft draft(String region) { return new Draft(region); }

    @Transient default Draft update() { return new Draft(this); }


    class Draft extends AbstractEntityDraft<Supply> {

        // Entity/Draft pattern-enabling methods:
        Draft(String region) {
            super(null);

            // Primary Key:
            set("id", UUIDs.timeBased());
            set("region", region);
        }

        Draft(Supply supply) {
            super((MapExportable) supply);
        }

        public Class<Supply> getEntityClass() { return Supply.class; }

        // Immutable properties:
        public UUID id() {
            return this.<UUID>get("id");
        }

        public String region() {
            return this.<String>get("region");
        }

        // Mutable properties:
        public String code() {
            return this.<String>get("code");
        }

        public Draft code(String code) {
            mutate("code", code);
            return this;
        }

        public Draft setCode(String code) {
            return code(code);
        }

        public String description() {
            return this.<String>get("description");
        }

        public Draft description(String description) {
            mutate("description", description);
            return this;
        }

        public Draft setDescription(String description) {
            return description(description);
        }

        public Map<String, Long> demand() {
            return this.<Map<String, Long>>get("demand");
        }

        public Draft demand(Map<String, Long> demand) {
            mutate("demand", demand);
            return this;
        }

        public Draft setDemand(Map<String, Long> demand) {
            return demand(demand);
        }

        public List<String> suppliers() {
            return this.<List<String>>get("suppliers");
        }

        public Draft suppliers(List<String> suppliers) {
            mutate("suppliers", suppliers);
            return this;
        }

        public Draft setSuppliers(List<String> suppliers) {
            return suppliers(suppliers);
        }

        public Set<String> shipments() {
            return this.<Set<String>>get("shipments");
        }

        public Draft shipments(Set<String> shipments) {
            mutate("shipments", shipments);
            return this;
        }

        public Draft setshipments(Set<String> shipments) {
            return shipments(shipments);
        }

    }
}
