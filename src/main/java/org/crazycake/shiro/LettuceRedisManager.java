package org.crazycake.shiro;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.crazycake.shiro.common.AbstractLettuceRedisManager;
import org.crazycake.shiro.exception.LettucePoolException;

import java.time.Duration;

/**
 * Singleton lettuce redis
 *
 * @author Teamo
 * @date 2022/05/18
 */
public class LettuceRedisManager extends AbstractLettuceRedisManager implements IRedisManager {
    private static final String DEFAULT_HOST = "127.0.0.1:6379";

    /**
     * redis host
     */
    private String host = DEFAULT_HOST;

    /**
     * timeout for RedisClient try to connect to redis server, not expire time! unit seconds
     */
    private long timeout = RedisURI.DEFAULT_TIMEOUT;

    /**
     * redis password
     */
    private String password;

    /**
     * redis database
     */
    private int database = 0;

    /**
     * GenericObjectPool
     */
    private volatile GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> genericObjectPool;

    protected void init() {
        if (genericObjectPool == null) {
            synchronized (LettuceRedisManager.class) {
                if (genericObjectPool == null) {
                    String[] hostAndPort = host.split(":");
                    RedisURI.Builder builder = RedisURI.builder();
                    builder.withHost(hostAndPort[0])
                            .withPort(Integer.parseInt(hostAndPort[1]))
                            .withDatabase(database)
                            .withTimeout(Duration.ofSeconds(timeout));
                    if (password != null) {
                        builder.withPassword(password.toCharArray());
                    }
                    RedisClient redisClient = RedisClient.create(builder.build());
                    redisClient.setOptions(getClientOptions());
                    genericObjectPool = ConnectionPoolSupport.createGenericObjectPool(() -> redisClient.connect(new ByteArrayCodec()), getGenericObjectPoolConfig());
                }
            }
        }
    }

    @Override
    protected StatefulRedisConnection<byte[], byte[]> getStatefulRedisConnection() {
        if (genericObjectPool == null) {
            init();
        }
        try {
            return genericObjectPool.borrowObject();
        } catch (Exception e) {
            throw new LettucePoolException("Could not get a resource from the pool", e);
        }
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> getGenericObjectPool() {
        return genericObjectPool;
    }

    public void setGenericObjectPool(GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> genericObjectPool) {
        this.genericObjectPool = genericObjectPool;
    }
}
