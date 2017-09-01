package net.helenus.core.operation;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public abstract class AbstractCache<K, V> {
  final Logger logger = LoggerFactory.getLogger(getClass());
  public Cache<K, V> cache;

  public AbstractCache() {
    RemovalListener<K, V> listener =
            new RemovalListener<K, V>() {
              @Override
              public void onRemoval(RemovalNotification<K, V> n) {
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

    V get(K key) {
        return cache.getIfPresent(key);
    }

    void put(K key, V value) {
        cache.put(key, value);
    }
}
