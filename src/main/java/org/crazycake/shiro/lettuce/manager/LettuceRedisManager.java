package org.crazycake.shiro.lettuce.manager;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.crazycake.shiro.exception.PoolException;
import org.crazycake.shiro.lettuce.AbstractLettuceRedisManager;

/**
 * Singleton lettuce redis
 *
 * @author Teamo
 * @date 2022/05/18
 */
public class LettuceRedisManager extends AbstractLettuceRedisManager<StatefulRedisConnection<byte[], byte[]>> {
    /**
     * Redis server host.
     */
    private String host = "localhost";

    /**
     * Redis server port.
     */
    private int port = 6379;

    private void initialize() {
        if (genericObjectPool == null) {
            synchronized (LettuceRedisManager.class) {
                if (genericObjectPool == null) {
                    RedisClient redisClient = RedisClient.create(createRedisURI());
                    redisClient.setOptions(getClientOptions());
                    genericObjectPool = ConnectionPoolSupport.createGenericObjectPool(() -> redisClient.connect(new ByteArrayCodec()), getGenericObjectPoolConfig());
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
}
