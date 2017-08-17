package net.helenus.core.operation;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import net.helenus.mapping.HelenusEntity;

import java.util.concurrent.ExecutionException;


public abstract class CacheManager {
    public enum Type { FETCH, MUTATE }

    private static CacheManager sessionFetch = new SessionCacheManager(Type.FETCH);

    protected CacheManager.Type type;


    public static CacheManager of(Type type, HelenusEntity entity) {
        if (entity != null && entity.isCacheable()) {
            return sessionFetch;
        }
        return null;
    }

    public CacheManager(Type type) {
        this.type = type;
    }

    protected abstract ResultSet fetch(Statement statement, OperationsDelegate delegate, ResultSetFuture resultSetFuture)
            throws InterruptedException, ExecutionException;
    protected abstract ResultSet mutate(Statement statement, OperationsDelegate delegate, ResultSetFuture resultSetFuture)
            throws InterruptedException, ExecutionException;

    public ResultSet apply(Statement statement, OperationsDelegate delegate, ResultSetFuture futureResultSet)
            throws InterruptedException, ExecutionException {
        ResultSet resultSet = null;
        switch (type) {
            case FETCH:
                 resultSet = fetch(statement, delegate, futureResultSet);
                break;
            case MUTATE:
                resultSet = mutate(statement, delegate, futureResultSet);
                break;
        }
        return resultSet;
    }

}
