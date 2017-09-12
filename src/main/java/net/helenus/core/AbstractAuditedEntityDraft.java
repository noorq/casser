package net.helenus.core;

import net.helenus.core.reflect.MapExportable;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;


public abstract class AbstractAuditedEntityDraft<E> extends AbstractEntityDraft<E> {

    public AbstractAuditedEntityDraft(MapExportable entity, AuditProvider auditProvider) {
        super(entity);

        Date in = new Date();
        LocalDateTime ldt = LocalDateTime.ofInstant(in.toInstant(), ZoneId.systemDefault());
        Date now = Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant());

        String who = auditProvider == null ? "unknown" : auditProvider.operatorName();

        if (entity == null) {
            set("createdBy", who);
            set("createdAt", now);
        }
        set("modifiedBy", who);
        set("modifiedAt", now);
    }

    public Date createdAt() {
        return (Date) get("createdAt", Date.class);
    }

}
