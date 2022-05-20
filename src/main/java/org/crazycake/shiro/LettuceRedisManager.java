package org.crazycake.shiro;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.crazycake.shiro.common.AbstractLettuceRedisManager;
import org.crazycake.shiro.exception.PoolException;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Singleton lettuce redis
 *
 * @author Teamo
 * @date 2022/05/18
 */
public class LettuceRedisManager extends AbstractLettuceRedisManager<StatefulRedisConnection<byte[], byte[]>> {

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
            e.printStackTrace();
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
            e.printStackTrace();
        }
        return dbSize;
    }

    @Override
    public Set<byte[]> keys(byte[] pattern) {
        Set<byte[]> keys = new HashSet<>(16);
        KeyScanCursor<byte[]> scanCursor = new KeyScanCursor<>();
        scanCursor.setCursor(ScanCursor.INITIAL.getCursor());
        ScanArgs scanArgs = ScanArgs.Builder.matches(pattern).limit(getCount());
        try (StatefulRedisConnection<byte[], byte[]> connect = getStatefulConnection()) {
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
