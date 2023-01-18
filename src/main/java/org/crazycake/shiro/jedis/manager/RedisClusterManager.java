package org.crazycake.shiro.jedis.manager;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.crazycake.shiro.IRedisManager;
import redis.clients.jedis.*;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RedisClusterManager implements IRedisManager {

    private static final int DEFAULT_COUNT = 100;
    private static final int DEFAULT_MAX_ATTEMPTS = 3;
    private static final String DEFAULT_HOST = "127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002";
    private String host = DEFAULT_HOST;

    // timeout for jedis try to connect to redis server, not expire time! In milliseconds
    private int timeout = Protocol.DEFAULT_TIMEOUT;

    // timeout for jedis try to read data from redis server
    private int soTimeout = Protocol.DEFAULT_TIMEOUT;

    private String password;

    private int database = Protocol.DEFAULT_DATABASE;

    private int count = DEFAULT_COUNT;

    private int maxAttempts = DEFAULT_MAX_ATTEMPTS;

    private GenericObjectPoolConfig<Connection> genericObjectPoolConfig = new GenericObjectPoolConfig<>();

    private volatile JedisCluster jedisCluster = null;

    private void init() {
        if (jedisCluster == null) {
            synchronized (RedisClusterManager.class) {
                if (jedisCluster == null) {
                    jedisCluster = new JedisCluster(getHostAndPortSet(), timeout, soTimeout, maxAttempts, password, getGenericObjectPoolConfig());
                }
            }
        }
    }

    private Set<HostAndPort> getHostAndPortSet() {
        String[] hostAndPortArr = host.split(",");
        Set<HostAndPort> hostAndPorts = new HashSet<>(hostAndPortArr.length);
        for (String hostAndPortStr : hostAndPortArr) {
            String[] hostAndPort = hostAndPortStr.split(":");
            hostAndPorts.add(new HostAndPort(hostAndPort[0], Integer.parseInt(hostAndPort[1])));
        }
        return hostAndPorts;
    }

    protected JedisCluster getJedisCluster() {
        if (jedisCluster == null) {
            init();
        }
        return jedisCluster;
    }

    @Override
    public byte[] get(byte[] key) {
        if (key == null) {
            return null;
        }
        return getJedisCluster().get(key);
    }

    @Override
    public byte[] set(byte[] key, byte[] value, int expireTime) {
        if (key == null) {
            return null;
        }
        getJedisCluster().set(key, value);
        if (expireTime >= 0) {
            getJedisCluster().expire(key, expireTime);
        }
        return value;
    }

    @Override
    public void del(byte[] key) {
        if (key == null) {
            return;
        }
        getJedisCluster().del(key);
    }

    @Override
    public Long dbSize(byte[] pattern) {
        long dbSize = 0L;
        Map<String, ConnectionPool> clusterNodes = getJedisCluster().getClusterNodes();
        Set<Map.Entry<String, ConnectionPool>> entrySet = clusterNodes.entrySet();
        for (Map.Entry<String, ConnectionPool> node : entrySet) {
            long nodeDbSize = getDbSizeFromClusterNode(node.getValue(), pattern);
            if (nodeDbSize == 0L) {
                continue;
            }
            dbSize += nodeDbSize;
        }
        return dbSize;
    }

    @Override
    public Set<byte[]> keys(byte[] pattern) {
        Set<byte[]> keys = new HashSet<>();
        Map<String, ConnectionPool> clusterNodes = getJedisCluster().getClusterNodes();
        Set<Map.Entry<String, ConnectionPool>> entrySet = clusterNodes.entrySet();
        for (Map.Entry<String, ConnectionPool> node : entrySet) {
            Set<byte[]> nodeKeys = getKeysFromClusterNode(node.getValue(), pattern);
            if (nodeKeys.size() == 0) {
                continue;
            }
            keys.addAll(nodeKeys);
        }
        return keys;
    }

    private Set<byte[]> getKeysFromClusterNode(ConnectionPool connectionPool, byte[] pattern) {
        Set<byte[]> keys = new HashSet<>();
        try (Connection connection = connectionPool.getResource()) {
            ScanParams params = new ScanParams();
            params.count(count);
            params.match(pattern);
            byte[] cursor = ScanParams.SCAN_POINTER_START_BINARY;

            CommandArguments commandArguments;
            CommandObject<ScanResult<byte[]>> commandObject;
            ScanResult<byte[]> scanResult;
            do {
                commandArguments = new CommandArguments(Protocol.Command.SCAN).add(cursor).addParams(params);
                commandObject = new CommandObject<>(commandArguments, BuilderFactory.SCAN_BINARY_RESPONSE);
                scanResult = connection.executeCommand(commandObject);
                keys.addAll(scanResult.getResult());
                cursor = scanResult.getCursorAsBytes();
            } while (scanResult.getCursor().compareTo(ScanParams.SCAN_POINTER_START) > 0);
        }
        return keys;
    }

    private long getDbSizeFromClusterNode(ConnectionPool connectionPool, byte[] pattern) {
        long dbSize = 0L;
        try (Connection connection = connectionPool.getResource()) {
            ScanParams params = new ScanParams();
            params.count(count);
            params.match(pattern);
            byte[] cursor = ScanParams.SCAN_POINTER_START_BINARY;

            CommandArguments commandArguments;
            CommandObject<ScanResult<byte[]>> commandObject;
            ScanResult<byte[]> scanResult;
            do {
                commandArguments = new CommandArguments(Protocol.Command.SCAN).add(cursor).addParams(params);
                commandObject = new CommandObject<>(commandArguments, BuilderFactory.SCAN_BINARY_RESPONSE);
                scanResult = connection.executeCommand(commandObject);
                dbSize++;
                cursor = scanResult.getCursorAsBytes();
            } while (scanResult.getCursor().compareTo(ScanParams.SCAN_POINTER_START) > 0);
        }
        return dbSize;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getSoTimeout() {
        return soTimeout;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void setJedisCluster(JedisCluster jedisCluster) {
        this.jedisCluster = jedisCluster;
    }

    public GenericObjectPoolConfig<Connection> getGenericObjectPoolConfig() {
        return genericObjectPoolConfig;
    }

    public void setGenericObjectPoolConfig(GenericObjectPoolConfig<Connection> genericObjectPoolConfig) {
        this.genericObjectPoolConfig = genericObjectPoolConfig;
    }
}
