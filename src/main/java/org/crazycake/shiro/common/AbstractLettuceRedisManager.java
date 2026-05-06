package org.crazycake.shiro.common;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.crazycake.shiro.IRedisManager;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Base lettuce redis manager without connection pool.
 *
 * @author Teamo
 * @since 2022/05/19
 */
public abstract class AbstractLettuceRedisManager<T extends StatefulRedisConnection<byte[], byte[]>>
        implements IRedisManager, AutoCloseable {

    private static final int DEFAULT_COUNT = 100;

    /**
     * timeout for RedisClient to connect to redis server, not expire time.
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
     * Number of elements returned at every scan iteration.
     */
    private int count = DEFAULT_COUNT;

    /**
     * ClientOptions used to initialize RedisClient.
     */
    private ClientOptions clientOptions = ClientOptions.create();

    /**
     * Get shared connection.
     */
    protected abstract T getConnection();

    protected RedisCommands<byte[], byte[]> syncCommands() {
        return getConnection().sync();
    }

    protected void validateBaseConfiguration() {
        if (count <= 0) {
            throw new IllegalArgumentException("count must be greater than 0");
        }
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

    @Override
    public byte[] get(byte[] key) {
        if (key == null) {
            return null;
        }
        return syncCommands().get(key);
    }

    @Override
    public byte[] set(byte[] key, byte[] value, int expire) {
        if (key == null) {
            return null;
        }
        if (value == null) {
            throw new IllegalArgumentException("value must not be null");
        }

        RedisCommands<byte[], byte[]> sync = syncCommands();
        if (expire > 0) {
            sync.set(key, value, SetArgs.Builder.ex(expire));
        } else {
            sync.set(key, value);
        }
        return value;
    }

    @Override
    public void del(byte[] key) {
        if (key == null) {
            return;
        }
        syncCommands().del(key);
    }

    @Override
    public Long dbSize(byte[] pattern) {
        if (pattern == null) {
            return 0L;
        }

        long total = 0L;
        RedisCommands<byte[], byte[]> sync = syncCommands();
        ScanCursor cursor = ScanCursor.INITIAL;
        ScanArgs scanArgs = ScanArgs.Builder.matches(pattern).limit(count);

        do {
            KeyScanCursor<byte[]> keyScanCursor = sync.scan(cursor, scanArgs);
            total += keyScanCursor.getKeys().size();
            cursor = keyScanCursor;
        } while (!cursor.isFinished());

        return total;
    }

    @Override
    public Set<byte[]> keys(byte[] pattern) {
        if (pattern == null) {
            return Collections.emptySet();
        }

        Set<byte[]> keys = new HashSet<>();
        RedisCommands<byte[], byte[]> sync = syncCommands();
        ScanCursor cursor = ScanCursor.INITIAL;
        ScanArgs scanArgs = ScanArgs.Builder.matches(pattern).limit(count);

        do {
            KeyScanCursor<byte[]> keyScanCursor = sync.scan(cursor, scanArgs);
            keys.addAll(keyScanCursor.getKeys());
            cursor = keyScanCursor;
        } while (!cursor.isFinished());

        return keys;
    }
}
