package org.crazycake.shiro.lettuce.manager;

import io.lettuce.core.*;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.shiro.cache.CacheException;
import org.crazycake.shiro.IRedisManager;
import org.crazycake.shiro.exception.PoolException;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * @author Teamo
 * @date 2022/05/19
 */
public class LettuceRedisClusterManager implements IRedisManager {
    /**
     * Comma-separated list of "host:port" pairs to bootstrap from. This represents an
     * "initial" list of cluster nodes and is required to have at least one entry.
     */
    private List<String> nodes;

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
     * genericObjectPoolConfig used to initialize GenericObjectPoolConfig object.
     */
    private GenericObjectPoolConfig<StatefulRedisClusterConnection<byte[], byte[]>> genericObjectPoolConfig = new GenericObjectPoolConfig<>();

    /**
     * GenericObjectPool.
     */
    protected volatile GenericObjectPool<StatefulRedisClusterConnection<byte[], byte[]>> genericObjectPool;

    /**
     * clusterClientOptions used to initialize RedisClient.
     */
    private ClusterClientOptions clusterClientOptions = ClusterClientOptions.create();

    private void initialize() {
        if (genericObjectPool == null) {
            synchronized (LettuceRedisClusterManager.class) {
                if (genericObjectPool == null) {
                    RedisClusterClient redisClusterClient = RedisClusterClient.create(getClusterRedisURI());
                    redisClusterClient.setOptions(clusterClientOptions);
                    StatefulRedisClusterConnection<byte[], byte[]> connect = redisClusterClient.connect(new ByteArrayCodec());
                    genericObjectPool = ConnectionPoolSupport.createGenericObjectPool(() -> connect, genericObjectPoolConfig);
                }
            }
        }
    }

    private StatefulRedisClusterConnection<byte[], byte[]> getStatefulConnection() {
        if (genericObjectPool == null) {
            initialize();
        }
        try {
            return genericObjectPool.borrowObject();
        } catch (Exception e) {
            throw new PoolException("Could not get a resource from the pool", e);
        }
    }

    private List<RedisURI> getClusterRedisURI() {
        Objects.requireNonNull(nodes, "nodes must not be null!");
        return nodes.stream().map(node -> {
            String[] hostAndPort = node.split(":");
            RedisURI.Builder builder = RedisURI.builder()
                    .withHost(hostAndPort[0])
                    .withPort(Integer.parseInt(hostAndPort[0]))
                    .withDatabase(database)
                    .withTimeout(timeout);
            if (password != null) {
                builder.withPassword(password.toCharArray());
            }
            return builder.build();
        }).collect(Collectors.toList());
    }

    @Override
    public byte[] get(byte[] key) {
        if (key == null) {
            return null;
        }
        byte[] value = null;
        try (StatefulRedisClusterConnection<byte[], byte[]> connection = getStatefulConnection()) {
            if (isAsync) {
                RedisAdvancedClusterAsyncCommands<byte[], byte[]> async = connection.async();
                RedisFuture<byte[]> redisFuture = async.get(key);
                value = redisFuture.get();
            } else {
                RedisAdvancedClusterCommands<byte[], byte[]> sync = connection.sync();
                value = sync.get(key);
            }
        } catch (ExecutionException | InterruptedException e) {
            throw new CacheException(e);
        }
        return value;
    }

    @Override
    public byte[] set(byte[] key, byte[] value, int expire) {
        if (key == null) {
            return null;
        }
        try (StatefulRedisClusterConnection<byte[], byte[]> connection = getStatefulConnection()) {
            if (isAsync) {
                RedisAdvancedClusterAsyncCommands<byte[], byte[]> async = connection.async();
                async.set(key, value);
                if (expire > 0) {
                    async.expire(key, expire);
                }
            } else {
                RedisAdvancedClusterCommands<byte[], byte[]> sync = connection.sync();
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
        try (StatefulRedisClusterConnection<byte[], byte[]> connection = getStatefulConnection()) {
            if (isAsync) {
                RedisAdvancedClusterAsyncCommands<byte[], byte[]> async = connection.async();
                async.del(key);
            } else {
                RedisAdvancedClusterCommands<byte[], byte[]> sync = connection.sync();
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
        try (StatefulRedisClusterConnection<byte[], byte[]> connection = getStatefulConnection()) {
            while (!scanCursor.isFinished()) {
                scanCursor = getKeyScanCursor(connection, scanCursor, scanArgs);
                dbSize += scanCursor.getKeys().size();
            }
        } catch (ExecutionException | InterruptedException e) {
            throw new CacheException(e);
        }
        return dbSize;
    }

    @Override
    public Set<byte[]> keys(byte[] pattern) {
        Set<byte[]> keys = new HashSet<>(16);
        KeyScanCursor<byte[]> scanCursor = new KeyScanCursor<>();
        scanCursor.setCursor(ScanCursor.INITIAL.getCursor());
        ScanArgs scanArgs = ScanArgs.Builder.matches(pattern).limit(count);
        try (StatefulRedisClusterConnection<byte[], byte[]> connection = getStatefulConnection()) {
            while (!scanCursor.isFinished()) {
                scanCursor = getKeyScanCursor(connection, scanCursor, scanArgs);
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
     * @param connection connection
     * @param scanCursor scan cursor
     * @param scanArgs   scan param
     * @return KeyScanCursor
     * @throws ExecutionException   If the calculation throws an exception
     * @throws InterruptedException If the current thread is interrupted while waiting
     */
    private KeyScanCursor<byte[]> getKeyScanCursor(final StatefulRedisClusterConnection<byte[], byte[]> connection, KeyScanCursor<byte[]> scanCursor, ScanArgs scanArgs) throws ExecutionException, InterruptedException {
        if (isAsync) {
            RedisAdvancedClusterAsyncCommands<byte[], byte[]> async = connection.async();
            RedisFuture<KeyScanCursor<byte[]>> scan = async.scan(scanCursor, scanArgs);
            scanCursor = scan.get();
        } else {
            RedisAdvancedClusterCommands<byte[], byte[]> sync = connection.sync();
            scanCursor = sync.scan(scanCursor, scanArgs);
        }
        return scanCursor;
    }

    public List<String> getNodes() {
        return nodes;
    }

    public void setNodes(List<String> nodes) {
        this.nodes = nodes;
    }

    public ClusterClientOptions getClusterClientOptions() {
        return clusterClientOptions;
    }

    public void setClusterClientOptions(ClusterClientOptions clusterClientOptions) {
        this.clusterClientOptions = clusterClientOptions;
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

    public GenericObjectPoolConfig<StatefulRedisClusterConnection<byte[], byte[]>> getGenericObjectPoolConfig() {
        return genericObjectPoolConfig;
    }

    public void setGenericObjectPoolConfig(GenericObjectPoolConfig<StatefulRedisClusterConnection<byte[], byte[]>> genericObjectPoolConfig) {
        this.genericObjectPoolConfig = genericObjectPoolConfig;
    }

    public GenericObjectPool<StatefulRedisClusterConnection<byte[], byte[]>> getGenericObjectPool() {
        return genericObjectPool;
    }

    public void setGenericObjectPool(GenericObjectPool<StatefulRedisClusterConnection<byte[], byte[]>> genericObjectPool) {
        this.genericObjectPool = genericObjectPool;
    }
}
