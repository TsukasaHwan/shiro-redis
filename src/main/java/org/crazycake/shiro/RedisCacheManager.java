package org.crazycake.shiro;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.shiro.cache.Cache;
import org.apache.shiro.cache.CacheException;
import org.apache.shiro.cache.CacheManager;
import org.crazycake.shiro.serializer.ObjectSerializer;
import org.crazycake.shiro.serializer.RedisSerializer;
import org.crazycake.shiro.serializer.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RedisCacheManager
 *
 * @author Alexxiyang, Teamo
 */
public class RedisCacheManager implements CacheManager {

    private static final Logger logger = LoggerFactory.getLogger(RedisCacheManager.class);

    /**
     * fast lookup by name map
     */
    @SuppressWarnings("rawtypes")
    private final com.github.benmanes.caffeine.cache.Cache<String, Cache> caches = Caffeine.newBuilder().build();
    @SuppressWarnings("rawtypes")
    private RedisSerializer keySerializer = new StringSerializer();
    @SuppressWarnings("rawtypes")
    private RedisSerializer valueSerializer = new ObjectSerializer();

    private IRedisManager redisManager;

    /**
     * expire time in seconds
     */
    public static final int DEFAULT_EXPIRE = 1800;
    private int expire = DEFAULT_EXPIRE;

    /**
     * The Redis key prefix for caches
     */
    public static final String DEFAULT_CACHE_KEY_PREFIX = "shiro:cache:";
    private String keyPrefix = DEFAULT_CACHE_KEY_PREFIX;

    public static final String DEFAULT_PRINCIPAL_ID_FIELD_NAME = "id";
    private String principalIdFieldName = DEFAULT_PRINCIPAL_ID_FIELD_NAME;

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> Cache<K, V> getCache(String name) throws CacheException {
        logger.debug("get cache, name=" + name);

        return (Cache<K, V>) caches.get(name, key -> new RedisCache<>(redisManager, keySerializer, valueSerializer, keyPrefix + name + ":", expire, principalIdFieldName));
    }

    public IRedisManager getRedisManager() {
        return redisManager;
    }

    public void setRedisManager(IRedisManager redisManager) {
        this.redisManager = redisManager;
    }

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public void setKeyPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
    }

    public RedisSerializer<?> getKeySerializer() {
        return keySerializer;
    }

    public void setKeySerializer(RedisSerializer<?> keySerializer) {
        this.keySerializer = keySerializer;
    }

    public RedisSerializer<?> getValueSerializer() {
        return valueSerializer;
    }

    public void setValueSerializer(RedisSerializer<?> valueSerializer) {
        this.valueSerializer = valueSerializer;
    }

    public int getExpire() {
        return expire;
    }

    public void setExpire(int expire) {
        this.expire = expire;
    }

    public String getPrincipalIdFieldName() {
        return principalIdFieldName;
    }

    public void setPrincipalIdFieldName(String principalIdFieldName) {
        this.principalIdFieldName = principalIdFieldName;
    }
}
