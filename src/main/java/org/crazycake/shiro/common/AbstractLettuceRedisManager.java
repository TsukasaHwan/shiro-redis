package org.crazycake.shiro.common;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.crazycake.shiro.IRedisManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Abstract class of LettuceRedis.
 *
 * @author Teamo
 * @date 2022/05/18
 */
public abstract class AbstractLettuceRedisManager implements IRedisManager {
    private static final Logger log = LoggerFactory.getLogger(AbstractLettuceRedisManager.class);

    /**
     * We will operate redis through StatefulRedisConnection object.
     * Subclasses should obtain StatefulRedisConnection objects by implementing getStatefulRedisConnection().
     *
     * @return StatefulRedisConnection
     */
    protected abstract StatefulRedisConnection<byte[], byte[]> getStatefulRedisConnection();

    /**
     * Default value of count.
     */
    protected static final int DEFAULT_COUNT = 100;

    /**
     * The number of elements returned at every iteration.
     */
    private int count = DEFAULT_COUNT;

    /**
     * Default value of isAsync
     */
    protected static final boolean DEFAULT_IS_ASYNC = false;

    /**
     * Whether to enable async
     */
    private boolean isAsync = DEFAULT_IS_ASYNC;

    /**
     * ClientOptions used to initialize RedisClient.
     */
    private ClientOptions clientOptions = ClientOptions.builder().build();

    /**
     * genericObjectPoolConfig used to initialize GenericObjectPoolConfig object.
     */
    private GenericObjectPoolConfig<StatefulRedisConnection<byte[], byte[]>> genericObjectPoolConfig = new GenericObjectPoolConfig<>();

    @Override
    public byte[] get(byte[] key) {
        if (key == null) {
            return null;
        }
        byte[] value = null;
        try (StatefulRedisConnection<byte[], byte[]> connect = getStatefulRedisConnection()) {
            if (isAsync) {
                RedisAsyncCommands<byte[], byte[]> async = connect.async();
                RedisFuture<byte[]> redisFuture = async.get(key);
                value = redisFuture.get();
            } else {
                RedisCommands<byte[], byte[]> sync = connect.sync();
                value = sync.get(key);
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return value;
    }

    @Override
    public byte[] set(byte[] key, byte[] value, int expire) {
        if (key == null) {
            return null;
        }
        try (StatefulRedisConnection<byte[], byte[]> connect = getStatefulRedisConnection()) {
            if (isAsync) {
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
        try (StatefulRedisConnection<byte[], byte[]> connect = getStatefulRedisConnection()) {
            if (isAsync) {
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
        ScanArgs scanArgs = ScanArgs.Builder.matches(pattern).limit(count);
        try (StatefulRedisConnection<byte[], byte[]> connect = getStatefulRedisConnection()) {
            while (!scanCursor.isFinished()) {
                scanCursor = getKeyScanCursor(connect, scanCursor, scanArgs);
                dbSize += scanCursor.getKeys().size();
            }
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
        return dbSize;
    }

    @Override
    public Set<byte[]> keys(byte[] pattern) {
        Set<byte[]> keys = new HashSet<>(16);
        KeyScanCursor<byte[]> scanCursor = new KeyScanCursor<>();
        scanCursor.setCursor(ScanCursor.INITIAL.getCursor());
        ScanArgs scanArgs = ScanArgs.Builder.matches(pattern).limit(count);
        try (StatefulRedisConnection<byte[], byte[]> connect = getStatefulRedisConnection()) {
            while (!scanCursor.isFinished()) {
                scanCursor = getKeyScanCursor(connect, scanCursor, scanArgs);
                keys.addAll(scanCursor.getKeys());
            }
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
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
    private KeyScanCursor<byte[]> getKeyScanCursor(final StatefulRedisConnection<byte[], byte[]> connect, KeyScanCursor<byte[]> scanCursor, ScanArgs scanArgs) throws ExecutionException, InterruptedException {
        if (isAsync) {
            RedisAsyncCommands<byte[], byte[]> async = connect.async();
            RedisFuture<KeyScanCursor<byte[]>> scan = async.scan(scanCursor, scanArgs);
            scanCursor = scan.get();
        } else {
            RedisCommands<byte[], byte[]> sync = connect.sync();
            scanCursor = sync.scan(scanCursor, scanArgs);
        }
        return scanCursor;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public boolean isAsync() {
        return isAsync;
    }

    public void setIsAsync(boolean isAsync) {
        this.isAsync = isAsync;
    }

    public ClientOptions getClientOptions() {
        return clientOptions;
    }

    public void setClientOptions(ClientOptions clientOptions) {
        this.clientOptions = clientOptions;
    }

    public GenericObjectPoolConfig<StatefulRedisConnection<byte[], byte[]>> getGenericObjectPoolConfig() {
        return genericObjectPoolConfig;
    }

    public void setGenericObjectPoolConfig(GenericObjectPoolConfig<StatefulRedisConnection<byte[], byte[]>> genericObjectPoolConfig) {
        this.genericObjectPoolConfig = genericObjectPoolConfig;
    }
}
