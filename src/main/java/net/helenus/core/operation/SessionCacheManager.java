package net.helenus.core.operation;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import com.google.common.cache.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


public class SessionCacheManager extends CacheManager {
    final Logger logger = LoggerFactory.getLogger(getClass());

    private Cache<String, ResultSet> cache;

    SessionCacheManager(CacheManager.Type type) {
        super(type);

        RemovalListener<String, ResultSet> listener;
        listener = new RemovalListener<String, ResultSet>() {
            @Override
            public void onRemoval(RemovalNotification<String, ResultSet> n){
                if (n.wasEvicted()) {
                    String cause = n.getCause().name();
                    logger.info(cause);
                }
            }
        };

        cache = CacheBuilder.newBuilder()
                .maximumSize(10_000)
                .expireAfterAccess(20, TimeUnit.MINUTES)
                .weakKeys()
                .softValues()
                .removalListener(listener)
                .build();
    }

    protected ResultSet fetch(Statement statement, OperationsDelegate delegate, ResultSetFuture resultSetFuture)
            throws InterruptedException, ExecutionException {
        CacheKey key = delegate.getCacheKey();
        final String cacheKey = key == null ? statement.toString() : key.toString();
        ResultSet resultSet = null;
        if (cacheKey == null) {
            resultSet = resultSetFuture.get();
        } else {
            resultSet = cache.getIfPresent(cacheKey);
            if (resultSet == null) {
                resultSet = resultSetFuture.get();
                if (resultSet != null) {
                    cache.put(cacheKey, resultSet);
                }
            }
        }
        return resultSet;
    }

    protected ResultSet mutate(Statement statement, OperationsDelegate delegate, ResultSetFuture resultSetFuture)
            throws InterruptedException, ExecutionException {
        CacheKey key = delegate.getCacheKey();
        final String cacheKey = key == null ? statement.toString() : key.toString();
        ResultSet resultSet = resultSetFuture.get();
        if (cacheKey != null && resultSet != null) {
            cache.put(cacheKey, resultSet);
        }
        return resultSet;
    }

}
