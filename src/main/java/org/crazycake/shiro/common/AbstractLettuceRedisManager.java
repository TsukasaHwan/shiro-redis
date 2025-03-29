package org.crazycake.shiro.common;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.crazycake.shiro.IRedisManager;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author Teamo
 * @since 2022/05/19
 */
public abstract class AbstractLettuceRedisManager<T extends StatefulRedisConnection<byte[], byte[]>> implements IRedisManager, AutoCloseable {

    /**
     * Default value of count.
     */
    private static final int DEFAULT_COUNT = 100;

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
    private boolean isAsync = true;

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
     * Get a stateful connection.
     *
     * @return T
     */
    protected abstract T getStatefulConnection();

    /**
     * Return a stateful connection.
     *
     * @param connect T
     */
    protected abstract void returnObject(T connect);

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

    @Override
    public byte[] get(byte[] key) {
        if (key == null) {
            return null;
        }
        byte[] value = null;
        T connect = null;
        try {
            connect = getStatefulConnection();
            if (isAsync) {
                RedisAsyncCommands<byte[], byte[]> async = connect.async();
                RedisFuture<byte[]> redisFuture = async.get(key);
                value = LettuceFutures.awaitOrCancel(redisFuture, timeout.getSeconds(), TimeUnit.SECONDS);
            } else {
                RedisCommands<byte[], byte[]> sync = connect.sync();
                value = sync.get(key);
            }
        } finally {
            returnObject(connect);
        }
        return value;
    }

    @Override
    public byte[] set(byte[] key, byte[] value, int expire) {
        if (key == null) {
            return null;
        }
        T connect = null;
        try {
            connect = getStatefulConnection();
            if (isAsync) {
                RedisAsyncCommands<byte[], byte[]> async = connect.async();
                if (expire > 0) {
                    async.set(key, value, SetArgs.Builder.ex(expire));
                } else {
                    async.set(key, value);
                }
            } else {
                RedisCommands<byte[], byte[]> sync = connect.sync();
                if (expire > 0) {
                    sync.set(key, value, SetArgs.Builder.ex(expire));
                } else {
                    sync.set(key, value);
                }
            }
        } finally {
            returnObject(connect);
        }
        return value;
    }

    @Override
    public void del(byte[] key) {
        T connect = null;
        try {
            connect = getStatefulConnection();
            if (isAsync) {
                RedisAsyncCommands<byte[], byte[]> async = connect.async();
                async.del(key);
            } else {
                RedisCommands<byte[], byte[]> sync = connect.sync();
                sync.del(key);
            }
        } finally {
            returnObject(connect);
        }
    }

    @Override
    public Long dbSize(byte[] pattern) {
        long dbSize = 0L;
        KeyScanCursor<byte[]> scanCursor = new KeyScanCursor<>();
        scanCursor.setCursor(ScanCursor.INITIAL.getCursor());
        ScanArgs scanArgs = ScanArgs.Builder.matches(pattern).limit(count);
        T connect = null;
        try {
            connect = getStatefulConnection();
            while (!scanCursor.isFinished()) {
                scanCursor = getKeyScanCursor(connect, scanCursor, scanArgs);
                dbSize += scanCursor.getKeys().size();
            }
        } finally {
            returnObject(connect);
        }
        return dbSize;
    }

    @Override
    public Set<byte[]> keys(byte[] pattern) {
        Set<byte[]> keys = new HashSet<>();
        KeyScanCursor<byte[]> scanCursor = new KeyScanCursor<>();
        scanCursor.setCursor(ScanCursor.INITIAL.getCursor());
        ScanArgs scanArgs = ScanArgs.Builder.matches(pattern).limit(count);
        T connect = null;
        try {
            connect = getStatefulConnection();
            while (!scanCursor.isFinished()) {
                scanCursor = getKeyScanCursor(connect, scanCursor, scanArgs);
                keys.addAll(scanCursor.getKeys());
            }
        } finally {
            returnObject(connect);
        }
        return keys;
    }

    /**
     * get scan cursor result
     *
     * @param connect    connection
     * @param scanCursor scan cursor
     * @param scanArgs   scan param
     * @return KeyScanCursor
     */
    private KeyScanCursor<byte[]> getKeyScanCursor(final StatefulRedisConnection<byte[], byte[]> connect,
                                                   KeyScanCursor<byte[]> scanCursor,
                                                   ScanArgs scanArgs) {
        if (isAsync) {
            RedisAsyncCommands<byte[], byte[]> async = connect.async();
            scanCursor = LettuceFutures.awaitOrCancel(async.scan(scanCursor, scanArgs), timeout.getSeconds(), TimeUnit.SECONDS);
        } else {
            RedisCommands<byte[], byte[]> sync = connect.sync();
            scanCursor = sync.scan(scanCursor, scanArgs);
        }
        return scanCursor;
    }
}
