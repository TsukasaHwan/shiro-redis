package org.crazycake.shiro;

import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection;
import org.crazycake.shiro.common.AbstractLettuceRedisManager;

import java.util.List;
import java.util.Objects;

/**
 * Lettuce sentinel manager without connection pool.
 *
 * @author Teamo
 * @since 2022/05/19
 */
public class LettuceRedisSentinelManager
        extends AbstractLettuceRedisManager<StatefulRedisMasterReplicaConnection<byte[], byte[]>> {

    private static final String DEFAULT_MASTER_NAME = "mymaster";

    private String masterName = DEFAULT_MASTER_NAME;

    private List<String> nodes;

    private String sentinelPassword;

    private ReadFrom readFrom = ReadFrom.REPLICA_PREFERRED;

    private volatile RedisClient redisClient;

    private volatile StatefulRedisMasterReplicaConnection<byte[], byte[]> connection;

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

            RedisURI redisURI = createSentinelRedisURI();

            RedisClient client = RedisClient.create();
            client.setOptions(getClientOptions());

            StatefulRedisMasterReplicaConnection<byte[], byte[]> conn =
                    MasterReplica.connect(client, new ByteArrayCodec(), redisURI);
            conn.setReadFrom(readFrom);

            this.redisClient = client;
            this.connection = conn;
        }
    }

    private void validateConfiguration() {
        validateBaseConfiguration();

        Objects.requireNonNull(nodes, "nodes must not be null");
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be empty");
        }
        if (masterName == null || masterName.trim().isEmpty()) {
            throw new IllegalArgumentException("masterName must not be blank");
        }
    }

    @Override
    protected StatefulRedisMasterReplicaConnection<byte[], byte[]> getConnection() {
        if (closed) {
            throw new IllegalStateException("LettuceRedisSentinelManager is already closed");
        }
        if (connection == null) {
            initialize();
        }
        return connection;
    }

    @Override
    public void close() {
        closed = true;

        StatefulRedisMasterReplicaConnection<byte[], byte[]> conn = this.connection;
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

    private RedisURI createSentinelRedisURI() {
        Objects.requireNonNull(nodes, "nodes must not be null");

        RedisURI.Builder builder = RedisURI.builder();

        for (String node : nodes) {
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

            RedisURI.Builder sentinelBuilder = RedisURI.Builder.redis(host, port);
            if (sentinelPassword != null && !sentinelPassword.isEmpty()) {
                sentinelBuilder.withPassword(sentinelPassword.toCharArray());
            }

            builder.withSentinel(sentinelBuilder.build());
        }

        String password = getPassword();
        if (password != null && !password.isEmpty()) {
            builder.withPassword(password.toCharArray());
        }

        return builder.withSentinelMasterId(masterName)
                .withDatabase(getDatabase())
                .withTimeout(getTimeout())
                .build();
    }

    public String getMasterName() {
        return masterName;
    }

    public void setMasterName(String masterName) {
        this.masterName = masterName;
    }

    public List<String> getNodes() {
        return nodes;
    }

    public void setNodes(List<String> nodes) {
        this.nodes = nodes;
    }

    public String getSentinelPassword() {
        return sentinelPassword;
    }

    public void setSentinelPassword(String sentinelPassword) {
        this.sentinelPassword = sentinelPassword;
    }

    public ReadFrom getReadFrom() {
        return readFrom;
    }

    public void setReadFrom(ReadFrom readFrom) {
        this.readFrom = readFrom;
    }

    public RedisClient getRedisClient() {
        return redisClient;
    }

    public StatefulRedisMasterReplicaConnection<byte[], byte[]> getStatefulConnection() {
        return connection;
    }
}
