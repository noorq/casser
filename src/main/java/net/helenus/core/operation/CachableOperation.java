package net.helenus.core.operation;

import java.io.Serializable;

public interface CachableOperation {
    public <T extends Serializable> T getCacheKey();
    public <T extends Serializable> T valueToCache();
}
