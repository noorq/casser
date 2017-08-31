package net.helenus.core.operation;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import net.helenus.core.UnitOfWork;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class UnitOfWorkCache extends AbstractCache<String, ResultSet> {

    private final UnitOfWork uow;
    private final Map<String, ResultSet> cache = new HashMap<String, ResultSet>();
    private AbstractCache sessionCache;

    public UnitOfWorkCache(UnitOfWork uow, AbstractCache sessionCache) {
        super();
        this.sessionCache = sessionCache;
        this.uow = uow;
    }

    @Override
    protected ResultSet apply(Statement statement, OperationsDelegate delegate, ResultSetFuture resultSetFuture)
            throws InterruptedException, ExecutionException {
        return resultSetFuture.get();
        /*
        final CacheKey key = delegate.getCacheKey();
        final String cacheKey = (key == null) ? CacheKey.of(statement) : key.toString();
        ResultSet resultSet = null;
        if (cacheKey == null) {
            if (sessionCache != null) {
                ResultSet rs = sessionCache.apply(statement, delegate, resultSetFuture);
                if (rs != null) {
                    return rs;
                }
            }
        } else {
            resultSet = cache.get(cacheKey);
            if (resultSet != null) {
                return resultSet;
            }
        }
        resultSet = resultSetFuture.get();
        if (resultSet != null) {
            cache.put(cacheKey, resultSet);
        }
        return resultSet;
        */
    }

}
