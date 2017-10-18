package net.helenus.core.cache;

import net.helenus.mapping.HelenusProperty;

import java.util.Set;

public class EntityIdentifyingFacet extends Facet {

  public EntityIdentifyingFacet(HelenusProperty prop) {}

  public EntityIdentifyingFacet(HelenusProperty[]... props) {}

  public boolean isFullyBound() {
      return false;
  }

  public HelenusProperty getProperty() {
      return null;
  }

  public Set<HelenusProperty> getUnboundEntityProperties() {
      return null;
  }

  public void setValueForProperty(HelenusProperty prop, Object value) {
  }

}
