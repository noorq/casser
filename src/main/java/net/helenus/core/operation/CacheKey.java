package net.helenus.core.operation;

import com.datastax.driver.core.Statement;
import net.helenus.mapping.HelenusEntity;

import java.io.Serializable;

public class CacheKey implements Serializable {

  private String key;
  private HelenusEntity entity;

  CacheKey() {}

  CacheKey(HelenusEntity entity, String key) {
    this.entity = entity;
    this.key = key;
  }

  public void set(String key) {
    this.key = key;
  }

  public String toString() {
    return entity.getName() + "." + key;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    CacheKey cacheKey = (CacheKey) o;

    return key.equals(cacheKey.key);
  }

  @Override
  public int hashCode() {
    return key.hashCode();
  }

}
