package org.crazycake.shiro.lettuce;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.shiro.cache.CacheException;
import org.crazycake.shiro.IRedisManager;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * @author Teamo
 * @date 2022/05/19
 */
public abstract class AbstractLettuceRedisManager<T extends StatefulRedisConnection<byte[], byte[]>> implements IRedisManager {
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
     * Get a stateful connection.
     *
     * @return T
     */
    protected abstract T getStatefulConnection();

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
        try (StatefulRedisConnection<byte[], byte[]> connect = getStatefulConnection()) {
            if (isAsync()) {
                RedisAsyncCommands<byte[], byte[]> async = connect.async();
                RedisFuture<byte[]> redisFuture = async.get(key);
                value = redisFuture.get();
            } else {
                RedisCommands<byte[], byte[]> sync = connect.sync();
                value = sync.get(key);
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new CacheException(e);
        }
        return value;
    }

    @Override
    public byte[] set(byte[] key, byte[] value, int expire) {
        if (key == null) {
            return null;
        }
        try (StatefulRedisConnection<byte[], byte[]> connect = getStatefulConnection()) {
            if (isAsync()) {
                RedisAsyncCommands<byte[], byte[]> async = connect.async();
                async.set(key, value);
                if (expire > 0) {
                    async.expire(key, expire);
                }
            } else {
                RedisCommands<byte[], byte[]> sync = connect.sync();
                sync.set(key, value);
                if (expire > 0) {
                    sync.expire(key, expire);
                }
            }
        }
        return value;
    }

    @Override
    public void del(byte[] key) {
        try (StatefulRedisConnection<byte[], byte[]> connect = getStatefulConnection()) {
            if (isAsync()) {
                RedisAsyncCommands<byte[], byte[]> async = connect.async();
                async.del(key);
            } else {
                RedisCommands<byte[], byte[]> sync = connect.sync();
                sync.del(key);
            }
        }
    }

    @Override
    public Long dbSize(byte[] pattern) {
        long dbSize = 0L;
        KeyScanCursor<byte[]> scanCursor = new KeyScanCursor<>();
        scanCursor.setCursor(ScanCursor.INITIAL.getCursor());
        ScanArgs scanArgs = ScanArgs.Builder.matches(pattern).limit(getCount());
        try (StatefulRedisConnection<byte[], byte[]> connect = getStatefulConnection()) {
            while (!scanCursor.isFinished()) {
                scanCursor = getKeyScanCursor(connect, scanCursor, scanArgs);
                dbSize += scanCursor.getKeys().size();
            }
        } catch (ExecutionException | InterruptedException e) {
            throw new CacheException(e);
        }
        return dbSize;
    }

    @Override
    public Set<byte[]> keys(byte[] pattern) {
        Set<byte[]> keys = new HashSet<>();
        KeyScanCursor<byte[]> scanCursor = new KeyScanCursor<>();
        scanCursor.setCursor(ScanCursor.INITIAL.getCursor());
        ScanArgs scanArgs = ScanArgs.Builder.matches(pattern).limit(getCount());
        try (StatefulRedisConnection<byte[], byte[]> connect = getStatefulConnection()) {
            while (!scanCursor.isFinished()) {
                scanCursor = getKeyScanCursor(connect, scanCursor, scanArgs);
                keys.addAll(scanCursor.getKeys());
            }
        } catch (ExecutionException | InterruptedException e) {
            throw new CacheException(e);
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
     * @throws ExecutionException   If the calculation throws an exception
     * @throws InterruptedException If the current thread is interrupted while waiting
     */
    private KeyScanCursor<byte[]> getKeyScanCursor(final StatefulRedisConnection<byte[], byte[]> connect,
                                                   KeyScanCursor<byte[]> scanCursor,
                                                   ScanArgs scanArgs) throws ExecutionException, InterruptedException {
        if (isAsync()) {
            RedisAsyncCommands<byte[], byte[]> async = connect.async();
            RedisFuture<KeyScanCursor<byte[]>> scan = async.scan(scanCursor, scanArgs);
            scanCursor = scan.get();
        } else {
            RedisCommands<byte[], byte[]> sync = connect.sync();
            scanCursor = sync.scan(scanCursor, scanArgs);
        }
        return scanCursor;
    }
}
