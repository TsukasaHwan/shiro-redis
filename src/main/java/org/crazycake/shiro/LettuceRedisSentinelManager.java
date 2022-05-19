package org.crazycake.shiro;

import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.crazycake.shiro.exception.PoolException;

import java.util.Set;

/**
 * @author Teamo
 * @date 2022/05/19
 */
public class LettuceRedisSentinelManager extends LettuceRedisManager {
    private static final String DEFAULT_MASTER_NAME = "mymaster";

    private String masterName = DEFAULT_MASTER_NAME;

    private ReadFrom readFrom = ReadFrom.UPSTREAM;

    private void initialize() {
        if (genericObjectPool == null) {
            synchronized (LettuceRedisSentinelManager.class) {
                if (genericObjectPool == null) {
                    RedisURI redisURI = createRedisURI(new String[]{getHost(), String.valueOf(getPort())});
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
    protected RedisURI createRedisURI(String[] hostAndPort) {
        RedisURI.Builder builder = RedisURI.builder()
                .withDatabase(getDatabase())
                .withTimeout(getTimeout());
        String password = getPassword();
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
