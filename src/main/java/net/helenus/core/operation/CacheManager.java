package net.helenus.core.operation;

import com.datastax.driver.core.ResultSet;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import java.util.concurrent.TimeUnit;
import net.helenus.core.HelenusSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheManager {
  public enum Type {
    FETCH,
    MUTATE
  }

  final Logger logger = LoggerFactory.getLogger(getClass());
  final HelenusSession session;

  private AbstractCache sessionFetch;

  public CacheManager(HelenusSession session) {
    this.session = session;

    RemovalListener<String, ResultSet> listener =
        new RemovalListener<String, ResultSet>() {
          @Override
          public void onRemoval(RemovalNotification<String, ResultSet> n) {
            if (n.wasEvicted()) {
              String cause = n.getCause().name();
              logger.info(cause);
            }
          }
        };

    Cache<String, ResultSet> cache =
        CacheBuilder.newBuilder()
            .maximumSize(10_000)
            .expireAfterAccess(20, TimeUnit.MINUTES)
            .weakKeys()
            .softValues()
            .removalListener(listener)
            .build();

    sessionFetch = new SessionCache(Type.FETCH, this, cache);
  }

  public AbstractCache of(CacheManager.Type type) {
    return sessionFetch;
  }
}
