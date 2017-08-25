package net.helenus.core.operation;

import com.datastax.driver.core.Statement;

public class CacheKey {

  private String key;

  static String of(Statement statement) {
    return "use " + statement.getKeyspace() + "; " + statement.toString();
  }

  CacheKey() {}

  CacheKey(String key) {
    this.key = key;
  }

  public void set(String key) {
    this.key = key;
  }

  public String toString() {
    return key;
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
