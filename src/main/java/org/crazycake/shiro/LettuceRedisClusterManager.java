package org.crazycake.shiro;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.SetArgs;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.cluster.models.partitions.ClusterPartitionParser;
import io.lettuce.core.cluster.models.partitions.Partitions;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.codec.ByteArrayCodec;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Lettuce Redis Cluster manager without connection pool.
 *
 * @author Teamo
 * @since 2022/05/19
 */
public class LettuceRedisClusterManager implements IRedisManager, AutoCloseable {

    /**
     * Bootstrap nodes in host:port format.
     */
    private List<String> nodes;

    private static final int DEFAULT_COUNT = 100;

    /**
     * Connect timeout, not expire time.
     */
    private Duration timeout = RedisURI.DEFAULT_TIMEOUT_DURATION;

    private String password;

    /**
     * Scan count.
     */
    private int count = DEFAULT_COUNT;

    /**
     * Cluster client options.
     */
    private ClusterClientOptions clusterClientOptions = ClusterClientOptions.create();

    private volatile RedisClusterClient redisClusterClient;

    private volatile StatefulRedisClusterConnection<byte[], byte[]> connection;

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

            RedisClusterClient client = RedisClusterClient.create(getClusterRedisURI());
            client.setOptions(clusterClientOptions);

            StatefulRedisClusterConnection<byte[], byte[]> clusterConnection =
                    client.connect(new ByteArrayCodec());

            this.redisClusterClient = client;
            this.connection = clusterConnection;
        }
    }

    private void validateConfiguration() {
        Objects.requireNonNull(nodes, "nodes must not be null");
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be empty");
        }
        if (count <= 0) {
            throw new IllegalArgumentException("count must be greater than 0");
        }
    }

    private StatefulRedisClusterConnection<byte[], byte[]> getConnection() {
        if (closed) {
            throw new IllegalStateException("LettuceRedisClusterManager is already closed");
        }
        if (connection == null) {
            initialize();
        }
        return connection;
    }

    private RedisAdvancedClusterCommands<byte[], byte[]> syncCommands() {
        return getConnection().sync();
    }

    private List<RedisURI> getClusterRedisURI() {
        Objects.requireNonNull(nodes, "nodes must not be null");

        return nodes.stream().map(node -> {
            if (node == null || node.trim().isEmpty()) {
                throw new IllegalArgumentException("node must not be blank");
            }

            String[] hostAndPort = node.trim().split(":");
            if (hostAndPort.length != 2) {
                throw new IllegalArgumentException("Invalid node format: " + node + ", expected host:port");
            }

            String host = hostAndPort[0].trim();
            String portText = hostAndPort[1].trim();

            if (host.isEmpty()) {
                throw new IllegalArgumentException("Invalid node host: " + node);
            }

            int port;
            try {
                port = Integer.parseInt(portText);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid node port: " + node, e);
            }

            RedisURI.Builder builder = RedisURI.builder()
                    .withHost(host)
                    .withPort(port)
                    .withTimeout(timeout);

            if (password != null && !password.isEmpty()) {
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

        RedisAdvancedClusterCommands<byte[], byte[]> sync = syncCommands();
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

        AtomicLong total = new AtomicLong(0L);
        RedisAdvancedClusterCommands<byte[], byte[]> sync = syncCommands();
        Partitions partitions = ClusterPartitionParser.parse(sync.clusterNodes());

        partitions.forEach(node -> {
            if (!node.is(RedisClusterNode.NodeFlag.UPSTREAM)) {
                return;
            }

            RedisClusterCommands<byte[], byte[]> commands = sync.getConnection(node.getNodeId());
            ScanCursor cursor = ScanCursor.INITIAL;
            ScanArgs scanArgs = ScanArgs.Builder.matches(pattern).limit(count);

            do {
                KeyScanCursor<byte[]> keyScanCursor = commands.scan(cursor, scanArgs);
                total.addAndGet(keyScanCursor.getKeys().size());
                cursor = keyScanCursor;
            } while (!cursor.isFinished());
        });

        return total.get();
    }

    @Override
    public Set<byte[]> keys(byte[] pattern) {
        if (pattern == null) {
            return Collections.emptySet();
        }

        Set<byte[]> keys = new HashSet<>();
        RedisAdvancedClusterCommands<byte[], byte[]> sync = syncCommands();
        Partitions partitions = ClusterPartitionParser.parse(sync.clusterNodes());

        partitions.forEach(node -> {
            if (!node.is(RedisClusterNode.NodeFlag.UPSTREAM)) {
                return;
            }

            RedisClusterCommands<byte[], byte[]> commands = sync.getConnection(node.getNodeId());
            ScanCursor cursor = ScanCursor.INITIAL;
            ScanArgs scanArgs = ScanArgs.Builder.matches(pattern).limit(count);

            do {
                KeyScanCursor<byte[]> keyScanCursor = commands.scan(cursor, scanArgs);
                keys.addAll(keyScanCursor.getKeys());
                cursor = keyScanCursor;
            } while (!cursor.isFinished());
        });

        return keys;
    }

    @Override
    public void close() {
        closed = true;

        StatefulRedisClusterConnection<byte[], byte[]> conn = this.connection;
        RedisClusterClient client = this.redisClusterClient;

        this.connection = null;
        this.redisClusterClient = null;

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

    public void init() {
        initialize();
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

    public RedisClusterClient getRedisClusterClient() {
        return redisClusterClient;
    }

    public StatefulRedisClusterConnection<byte[], byte[]> getStatefulConnection() {
        return connection;
    }
}