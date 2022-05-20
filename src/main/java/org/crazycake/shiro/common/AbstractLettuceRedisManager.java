package org.crazycake.shiro.common;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.crazycake.shiro.IRedisManager;

import java.time.Duration;

/**
 * @author Teamo
 * @date 2022/05/19
 */
public abstract class AbstractLettuceRedisManager<T extends StatefulConnection<?, ?>> implements IRedisManager {
    /**
     * Default value of count.
     */
    private static final int DEFAULT_COUNT = 100;

    /**
     * Redis server host.
     */
    private String host = "localhost";

    /**
     * Redis server port.
     */
    private int port = 6379;

    /**
     * timeout for RedisClient try to connect to redis server, not expire time! unit seconds.
     */
    private Duration timeout = RedisURI.DEFAULT_TIMEOUT_DURATION;

    /**
     * Redis database.
     */
    private int database = 0;

    /**
     * Redis password.
     */
    private String password;

    /**
     * Whether to enable async.
     */
    private boolean isAsync = false;

    /**
     * The number of elements returned at every iteration.
     */
    private int count = DEFAULT_COUNT;

    /**
     * ClientOptions used to initialize RedisClient.
     */
    private ClientOptions clientOptions = ClientOptions.create();

    /**
     * genericObjectPoolConfig used to initialize GenericObjectPoolConfig object.
     */
    private GenericObjectPoolConfig<T> genericObjectPoolConfig = new GenericObjectPoolConfig<>();

    /**
     * GenericObjectPool.
     */
    protected volatile GenericObjectPool<T> genericObjectPool;

    /**
     * Get a stateful connection.
     *
     * @return T
     */
    protected abstract T getStatefulConnection();

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

    public Duration getTimeout() {
        return timeout;
    }

    public void setTimeout(Duration timeout) {
        this.timeout = timeout;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isAsync() {
        return isAsync;
    }

    public void setIsAsync(boolean isAsync) {
        this.isAsync = isAsync;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public ClientOptions getClientOptions() {
        return clientOptions;
    }

    public void setClientOptions(ClientOptions clientOptions) {
        this.clientOptions = clientOptions;
    }

    public GenericObjectPoolConfig<T> getGenericObjectPoolConfig() {
        return genericObjectPoolConfig;
    }

    public void setGenericObjectPoolConfig(GenericObjectPoolConfig<T> genericObjectPoolConfig) {
        this.genericObjectPoolConfig = genericObjectPoolConfig;
    }
}
