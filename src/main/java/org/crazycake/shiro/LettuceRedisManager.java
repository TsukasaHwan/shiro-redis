package org.crazycake.shiro;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import org.crazycake.shiro.common.AbstractLettuceRedisManager;

/**
 * Standalone lettuce redis manager without connection pool.
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

    private volatile RedisClient redisClient;

    private volatile StatefulRedisConnection<byte[], byte[]> connection;

    private final Object initLock = new Object();

    private volatile boolean closed = false;

    private void initialize() {
        if (connection != null) {
            return;
        }

        synchronized (initLock) {
            if (connection != null) {
                return;
            }

            validateConfiguration();

            RedisClient client = RedisClient.create(createRedisURI());
            client.setOptions(getClientOptions());

            StatefulRedisConnection<byte[], byte[]> redisConnection = client.connect(new ByteArrayCodec());

            this.redisClient = client;
            this.connection = redisConnection;
        }
    }

    private void validateConfiguration() {
        validateBaseConfiguration();

        if (host == null || host.trim().isEmpty()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
    }

    private RedisURI createRedisURI() {
        RedisURI.Builder builder = RedisURI.builder()
                .withHost(getHost())
                .withPort(getPort())
                .withDatabase(getDatabase())
                .withTimeout(getTimeout());

        String password = getPassword();
        if (password != null && !password.isEmpty()) {
            builder.withPassword(password.toCharArray());
        }
        return builder.build();
    }

    @Override
    protected StatefulRedisConnection<byte[], byte[]> getConnection() {
        if (closed) {
            throw new IllegalStateException("LettuceRedisManager is already closed");
        }
        if (connection == null) {
            initialize();
        }
        return connection;
    }

    @Override
    public void close() {
        closed = true;

        StatefulRedisConnection<byte[], byte[]> conn = this.connection;
        RedisClient client = this.redisClient;

        this.connection = null;
        this.redisClient = null;

        if (conn != null) {
            try {
                conn.close();
            } catch (Exception ignored) {
            }
        }

        if (client != null) {
            try {
                client.shutdown();
            } catch (Exception ignored) {
            }
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

    public RedisClient getRedisClient() {
        return redisClient;
    }

    public StatefulRedisConnection<byte[], byte[]> getStatefulConnection() {
        return connection;
    }
}
