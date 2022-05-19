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
    protected static final int DEFAULT_COUNT = 100;

    /**
     * Redis server host.
     */
    protected String host = "localhost";

    /**
     * Redis server port.
     */
    protected int port = 6379;

    /**
     * timeout for RedisClient try to connect to redis server, not expire time! unit seconds.
     */
    protected Duration timeout = RedisURI.DEFAULT_TIMEOUT_DURATION;

    /**
     * Redis database.
     */
    protected int database = 0;

    /**
     * Redis password.
     */
    protected String password;

    /**
     * Whether to enable async.
     */
    protected boolean isAsync = false;

    /**
     * The number of elements returned at every iteration.
     */
    protected int count = DEFAULT_COUNT;

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
     * We will operate redis through StatefulRedisConnection object.
     * Subclasses should obtain StatefulConnection objects by implementing getStatefulConnection().
     *
     * @return StatefulConnection
     */
    protected abstract T getStatefulConnection();

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
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

    public GenericObjectPool<T> getGenericObjectPool() {
        return genericObjectPool;
    }

    public void setGenericObjectPool(GenericObjectPool<T> genericObjectPool) {
        this.genericObjectPool = genericObjectPool;
    }

    /**
     * create RedisURI
     *
     * @param hostAndPort host port string
     * @return RedisURI
     */
    protected RedisURI createRedisURI(String[] hostAndPort) {
        RedisURI.Builder builder = RedisURI.builder()
                .withHost(hostAndPort[0])
                .withPort(Integer.parseInt(hostAndPort[1]))
                .withDatabase(database)
                .withTimeout(timeout);
        if (password != null) {
            builder.withPassword(password.toCharArray());
        }
        return builder.build();
    }
}
