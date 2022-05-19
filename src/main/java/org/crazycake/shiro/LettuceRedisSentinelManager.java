package org.crazycake.shiro;

import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.crazycake.shiro.exception.PoolException;

import java.time.Duration;
import java.util.Set;

/**
 * @author Teamo
 * @date 2022/05/19
 */
public class LettuceRedisSentinelManager extends LettuceRedisManager {
    private static final String DEFAULT_HOST = "127.0.0.1:26379";

    private static final String DEFAULT_MASTER_NAME = "mymaster";

    private String masterName = DEFAULT_MASTER_NAME;

    private ReadFrom readFrom = ReadFrom.UPSTREAM;

    private void init() {
        if (genericObjectPool == null) {
            synchronized (LettuceRedisSentinelManager.class) {
                if (genericObjectPool == null) {
                    if (host == null) {
                        host = DEFAULT_HOST;
                    }
                    String[] hostAndPort = host.split(":");
                    RedisURI redisURI = createRedisURI(hostAndPort);
                    RedisClient redisClient = RedisClient.create(redisURI);
                    redisClient.setOptions(getClientOptions());
                    StatefulRedisMasterReplicaConnection<byte[], byte[]> connect = MasterReplica.connect(redisClient, new ByteArrayCodec(), redisURI);
                    connect.setReadFrom(readFrom);
                    genericObjectPool = ConnectionPoolSupport.createGenericObjectPool(() -> connect, getGenericObjectPoolConfig());
                }
            }
        }
    }

    @Override
    protected StatefulRedisMasterReplicaConnection<byte[], byte[]> getStatefulConnection() {
        if (genericObjectPool == null) {
            init();
        }
        try {
            return (StatefulRedisMasterReplicaConnection<byte[], byte[]>) genericObjectPool.borrowObject();
        } catch (Exception e) {
            throw new PoolException("Could not get a resource from the pool", e);
        }
    }

    @Override
    protected RedisURI createRedisURI(String[] hostAndPort) {
        RedisURI.Builder builder = RedisURI.builder()
                .withDatabase(database)
                .withTimeout(Duration.ofSeconds(timeout));
        if (password != null) {
            builder.withPassword(password.toCharArray());
        }
        return builder.withSentinel(hostAndPort[0], Integer.parseInt(hostAndPort[1])).withSentinelMasterId(masterName).build();
    }

    @Override
    public byte[] get(byte[] key) {
        return super.get(key);
    }

    @Override
    public byte[] set(byte[] key, byte[] value, int expire) {
        return super.set(key, value, expire);
    }

    @Override
    public void del(byte[] key) {
        super.del(key);
    }

    @Override
    public Long dbSize(byte[] pattern) {
        return super.dbSize(pattern);
    }

    @Override
    public Set<byte[]> keys(byte[] pattern) {
        return super.keys(pattern);
    }

    public String getMasterName() {
        return masterName;
    }

    public void setMasterName(String masterName) {
        this.masterName = masterName;
    }

    public ReadFrom getReadFrom() {
        return readFrom;
    }

    public void setReadFrom(ReadFrom readFrom) {
        this.readFrom = readFrom;
    }
}
