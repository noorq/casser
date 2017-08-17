package net.helenus.core.operation;

public class CacheKey {

    private String key;

    CacheKey() {}

    CacheKey(String key) { this.key = key; }

    public void set(String key) { this.key = key; }

    public String toString() { return key; }

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
