package net.helenus.test.integration.core.draft;

import java.util.UUID;

import net.helenus.core.AbstractAuditedEntityDraft;
import net.helenus.core.reflect.MapExportable;
import net.helenus.mapping.annotation.*;


@Table
public interface Inventory {

    @PartitionKey  UUID id();
    @Column("emea") @Types.Counter long EMEA();
    @Column("noram") @Types.Counter long NORAM();
    @Column("apac") @Types.Counter long APAC();

    @Transient static Draft draft(UUID id) { return new Draft(id); }

    @Transient default Draft update() { return new Draft(this); }


    class Draft extends AbstractAuditedEntityDraft<Inventory> {

        // Entity/Draft pattern-enabling methods:
        Draft(UUID id) {
            super(null);

            // Primary Key:
            set("id", id);
        }

        Draft(Inventory inventory) {
            super((MapExportable) inventory);
        }

        public Class<Inventory> getEntityClass() { return Inventory.class; }

        protected String getCurrentAuditor() { return "unknown"; }

        // Immutable properties:
        public UUID id() {
            return this.<UUID>get("id", UUID.class);
        }

        public long EMEA() {
            return this.<Long>get("EMEA", long.class);
        }

        public Draft EMEA(long count) {
            mutate("EMEA", count);
            return this;
        }

        public long APAC() {
            return this.<Long>get("APAC", long.class);
        }

        public Draft APAC(long count) {
            mutate("APAC", count);
            return this;
        }

        public long NORAM() {
            return this.<Long>get("NORAM", long.class);
        }

        public Draft NORAM(long count) {
            mutate("NORAM", count);
            return this;
        }

    }

}
