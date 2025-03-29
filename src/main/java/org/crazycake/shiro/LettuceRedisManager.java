package org.crazycake.shiro;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.crazycake.shiro.common.AbstractLettuceRedisManager;
import org.crazycake.shiro.exception.PoolException;

/**
 * Singleton lettuce redis
 *
 * @author Teamo
 * @since 2022/05/18
 */
public class LettuceRedisManager extends AbstractLettuceRedisManager<StatefulRedisConnection<byte[], byte[]>> {

    /**
     * Redis server host.
     */
    private String host = "localhost";

    /**
     * Redis server port.
     */
    private int port = RedisURI.DEFAULT_REDIS_PORT;

    /**
     * GenericObjectPool.
     */
    private volatile GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> genericObjectPool;

    /**
     * Redis client.
     */
    private RedisClient redisClient;

    private void initialize() {
        if (genericObjectPool == null) {
            synchronized (LettuceRedisManager.class) {
                if (genericObjectPool == null) {
                    redisClient = RedisClient.create(createRedisURI());
                    redisClient.setOptions(getClientOptions());
                    GenericObjectPoolConfig<StatefulRedisConnection<byte[], byte[]>> genericObjectPoolConfig = getGenericObjectPoolConfig();
                    genericObjectPool = ConnectionPoolSupport.createGenericObjectPool(() -> redisClient.connect(new ByteArrayCodec()), genericObjectPoolConfig);
                }
            }
        }
    }

    private RedisURI createRedisURI() {
        RedisURI.Builder builder = RedisURI.builder()
                .withHost(getHost())
                .withPort(getPort())
                .withDatabase(getDatabase())
                .withTimeout(getTimeout());
        String password = getPassword();
        if (password != null) {
            builder.withPassword(password.toCharArray());
        }
        return builder.build();
    }

    @Override
    protected StatefulRedisConnection<byte[], byte[]> getStatefulConnection() {
        if (genericObjectPool == null) {
            initialize();
        }
        try {
            return genericObjectPool.borrowObject();
        } catch (Exception e) {
            throw new PoolException("Could not get a resource from the pool", e);
        }
    }

    @Override
    protected void returnObject(StatefulRedisConnection<byte[], byte[]> connect) {
        if (connect != null) {
            genericObjectPool.returnObject(connect);
        }
    }

    @Override
    public void close() throws Exception {
        if (genericObjectPool != null) {
            genericObjectPool.close();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> getGenericObjectPool() {
        return genericObjectPool;
    }

    public void setGenericObjectPool(GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> genericObjectPool) {
        this.genericObjectPool = genericObjectPool;
    }
}
