package net.helenus.core.operation;

import java.util.Optional;

import net.helenus.core.UnitOfWork;

public class UnitOfWorkCache extends AbstractCache<CacheKey, Object> {

    private final UnitOfWork uow;
    private AbstractCache<CacheKey, Object> sessionCache;

    public UnitOfWorkCache(UnitOfWork uow, AbstractCache sessionCache) {
        super();
        this.sessionCache = sessionCache;
        this.uow = uow;
    }

    @Override
    Object get(CacheKey key) {
        Object result = null;
        UnitOfWork parent = null;
        do {
            result = uow.getCache().get(key);
            parent = uow.getEnclosingUnitOfWork();
        } while(result == null && parent != null);
        if (result == null) {
            result = sessionCache.get(key);
        }
        return result;
    }

    @Override
    void put(CacheKey key, Object result) {
        cache.put(key, result);
    }

}
