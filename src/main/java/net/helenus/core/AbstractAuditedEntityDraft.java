package net.helenus.core;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import net.helenus.core.reflect.MapExportable;

public abstract class AbstractAuditedEntityDraft<E> extends AbstractEntityDraft<E> {

  public AbstractAuditedEntityDraft(MapExportable entity) {
    super(entity);

    Date in = new Date();
    LocalDateTime ldt = LocalDateTime.ofInstant(in.toInstant(), ZoneId.systemDefault());
    Date now = Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant());

    String who = getCurrentAuditor();

    if (entity == null) {
      if (who != null) {
        set("createdBy", who);
      }
      set("createdAt", now);
    }
    if (who != null) {
      set("modifiedBy", who);
    }
    set("modifiedAt", now);
  }

  protected String getCurrentAuditor() {
    return null;
  }

  public Date createdAt() {
    return (Date) get("createdAt", Date.class);
  }
}
