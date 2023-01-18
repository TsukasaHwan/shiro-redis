package org.crazycake.shiro;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.shiro.session.Session;
import org.apache.shiro.session.UnknownSessionException;
import org.apache.shiro.session.mgt.eis.AbstractSessionDAO;
import org.crazycake.shiro.common.SessionInMemory;
import org.crazycake.shiro.exception.SerializationException;
import org.crazycake.shiro.serializer.ObjectSerializer;
import org.crazycake.shiro.serializer.RedisSerializer;
import org.crazycake.shiro.serializer.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * Used for setting/getting authentication information from Redis
 *
 * @author Alexy Ynag, Teamo
 */
public class RedisSessionDAO extends AbstractSessionDAO {

    private static final Logger logger = LoggerFactory.getLogger(RedisSessionDAO.class);

    private static final String DEFAULT_SESSION_KEY_PREFIX = "shiro:session:";
    private String keyPrefix = DEFAULT_SESSION_KEY_PREFIX;

    /**
     * doReadSession be called about 10 times when login.
     * Save Session in ThreadLocal to resolve this problem. sessionInMemoryTimeout is expiration of Session in ThreadLocal.
     * The default value is 1000 milliseconds (1s).
     * Most of time, you don't need to change it.
     * <p>
     * You can turn it off by setting sessionInMemoryEnabled to false
     */
    private static final long DEFAULT_SESSION_IN_MEMORY_TIMEOUT = 1000L;
    private long sessionInMemoryTimeout = DEFAULT_SESSION_IN_MEMORY_TIMEOUT;

    private static final boolean DEFAULT_SESSION_IN_MEMORY_ENABLED = true;
    private boolean sessionInMemoryEnabled = DEFAULT_SESSION_IN_MEMORY_ENABLED;

    private static final ThreadLocal<Cache<Serializable, SessionInMemory>> SESSIONS_IN_THREAD = new ThreadLocal<>();

    /**
     * expire time in seconds.
     * NOTE: Please make sure expire is longer than session.getTimeout(),
     * otherwise you might need the issue that session in Redis got erased when the Session is still available
     * <p>
     * DEFAULT_EXPIRE: use the timeout of session instead of setting it by yourself
     * NO_EXPIRE: never expire
     */
    private static final int DEFAULT_EXPIRE = -2;
    private static final int NO_EXPIRE = -1;
    private int expire = DEFAULT_EXPIRE;

    private static final long MILLISECONDS_IN_A_SECOND = 1000L;

    /**
     * redisManager used for communicate with Redis
     */
    private IRedisManager redisManager;

    /**
     * Serializer of key
     */
    @SuppressWarnings("rawtypes")
    private RedisSerializer keySerializer = new StringSerializer();

    /**
     * Serializer of value
     */
    @SuppressWarnings("rawtypes")
    private RedisSerializer valueSerializer = new ObjectSerializer();

    /**
     * save/update session
     *
     * @param session session {@link Session}
     * @throws UnknownSessionException UnknownSessionException
     */
    @Override
    public void update(Session session) throws UnknownSessionException {
        if (this.sessionInMemoryEnabled) {
            this.doRemoveSessionInMemory();
        }
        this.saveSession(session);
        if (this.sessionInMemoryEnabled) {
            this.setSessionToThreadLocal(session.getId(), session);
        }
    }

    /**
     * Save session to redis
     *
     * @param session session {@link Session}
     * @throws UnknownSessionException Returned when the session is invalid or session storage fails
     */
    @SuppressWarnings("unchecked")
    private void saveSession(Session session) throws UnknownSessionException {
        if (session == null || session.getId() == null) {
            logger.error("session or session id is null");
            throw new UnknownSessionException("session or session id is null");
        }
        byte[] key;
        byte[] value;
        try {
            key = keySerializer.serialize(getRedisSessionKey(session.getId()));
            value = valueSerializer.serialize(session);
        } catch (SerializationException e) {
            logger.error("serialize session error. session id=" + session.getId());
            throw new UnknownSessionException(e);
        }
        if (expire == DEFAULT_EXPIRE) {
            redisManager.set(key, value, (int) (session.getTimeout() / MILLISECONDS_IN_A_SECOND));
            return;
        }
        if (expire != NO_EXPIRE && expire * MILLISECONDS_IN_A_SECOND < session.getTimeout()) {
            logger.warn("Redis session expire time: "
                        + (expire * MILLISECONDS_IN_A_SECOND)
                        + " is less than Session timeout: "
                        + session.getTimeout()
                        + " . It may cause some problems.");
        }
        redisManager.set(key, value, expire);
    }

    /**
     * Delete session
     *
     * @param session session object {@link Session}
     */
    @Override
    @SuppressWarnings("unchecked")
    public void delete(Session session) {
        if (this.sessionInMemoryEnabled) {
            this.doRemoveSessionInMemory();
        }
        if (session == null || session.getId() == null) {
            logger.error("session or session id is null");
            return;
        }
        if (this.sessionInMemoryEnabled) {
            this.delSessionFromThreadLocal(session.getId());
        }
        try {
            redisManager.del(keySerializer.serialize(getRedisSessionKey(session.getId())));
        } catch (SerializationException e) {
            logger.error("delete session error. session id=" + session.getId());
        }
    }

    /**
     * Get all active sessions
     *
     * @return return session collection
     */
    @Override
    @SuppressWarnings("unchecked")
    public Collection<Session> getActiveSessions() {
        if (this.sessionInMemoryEnabled) {
            this.doRemoveSessionInMemory();
        }
        Set<Session> sessions = new HashSet<Session>();
        try {
            Set<byte[]> keys = redisManager.keys(keySerializer.serialize(this.keyPrefix + "*"));
            if (keys != null && keys.size() > 0) {
                for (byte[] key : keys) {
                    Session s = (Session) valueSerializer.deserialize(redisManager.get(key));
                    sessions.add(s);
                }
            }
        } catch (SerializationException e) {
            logger.error("get active sessions error.");
        }
        return sessions;
    }

    /**
     * Create session object
     *
     * @param session {@link Session}
     * @return session id
     */
    @Override
    protected Serializable doCreate(Session session) {
        if (this.sessionInMemoryEnabled) {
            this.doRemoveSessionInMemory();
        }
        if (session == null) {
            logger.error("session is null");
            throw new UnknownSessionException("session is null");
        }
        Serializable sessionId = this.generateSessionId(session);
        this.assignSessionId(session, sessionId);
        this.saveSession(session);
        return sessionId;
    }

    /**
     * I change
     *
     * @param sessionId session id
     * @return {@link Session}
     */
    @Override
    @SuppressWarnings("unchecked")
    protected Session doReadSession(Serializable sessionId) {
        if (this.sessionInMemoryEnabled) {
            this.doRemoveSessionInMemory();
        }
        if (sessionId == null) {
            logger.warn("session id is null");
            return null;
        }
        if (this.sessionInMemoryEnabled) {
            Session session = getSessionFromThreadLocal(sessionId);
            if (session != null) {
                return session;
            }
        }
        Session session = null;
        try {
            String sessionRedisKey = getRedisSessionKey(sessionId);
            logger.debug("read session: " + sessionRedisKey + " from Redis");
            session = (Session) valueSerializer.deserialize(redisManager.get(keySerializer.serialize(sessionRedisKey)));
            if (this.sessionInMemoryEnabled) {
                setSessionToThreadLocal(sessionId, session);
            }
        } catch (SerializationException e) {
            logger.error("read session error. sessionId: " + sessionId);
        }
        return session;
    }

    /**
     * Set session to local thread
     *
     * @param sessionId session id
     * @param session   session {@link Session}
     */
    private void setSessionToThreadLocal(Serializable sessionId, Session session) {
        this.initSessionsInThread();
        Cache<Serializable, SessionInMemory> sessionCache = SESSIONS_IN_THREAD.get();
        sessionCache.put(sessionId, new SessionInMemory(session));
    }

    /**
     * Delete session from local thread
     *
     * @param sessionId session id
     */
    private void delSessionFromThreadLocal(Serializable sessionId) {
        Cache<Serializable, SessionInMemory> sessionCache = SESSIONS_IN_THREAD.get();
        if (sessionId == null || sessionCache == null) {
            return;
        }
        sessionCache.invalidate(sessionId);
    }

    /**
     * Initialize memory session storage object
     */
    private void initSessionsInThread() {
        Cache<Serializable, SessionInMemory> sessionMap = SESSIONS_IN_THREAD.get();
        if (sessionMap == null) {
            sessionMap = Caffeine.newBuilder()
                    .expireAfterWrite(Duration.ofMillis(sessionInMemoryTimeout))
                    .build();

            SESSIONS_IN_THREAD.set(sessionMap);
        }
    }

    /**
     * Manually clean up invalid memory sessions
     */
    private void doRemoveSessionInMemory() {
        Cache<Serializable, SessionInMemory> sessionCache = SESSIONS_IN_THREAD.get();
        if (sessionCache == null) {
            return;
        }

        sessionCache.cleanUp();

        ConcurrentMap<Serializable, SessionInMemory> sessionMap = sessionCache.asMap();
        if (sessionMap.isEmpty()) {
            SESSIONS_IN_THREAD.remove();
        }
    }

    /**
     * Get the session from memory according to the session id
     *
     * @param sessionId session id
     * @return session {@link Session}
     */
    private Session getSessionFromThreadLocal(Serializable sessionId) {
        Cache<Serializable, SessionInMemory> sessionCache = SESSIONS_IN_THREAD.get();
        if (sessionId == null || sessionCache == null) {
            return null;
        }

        SessionInMemory sessionInMemory = sessionCache.getIfPresent(sessionId);
        if (sessionInMemory == null) {
            return null;
        }

        logger.debug("read session from memory");
        return sessionInMemory.getSession();
    }

    private String getRedisSessionKey(Serializable sessionId) {
        return this.keyPrefix + sessionId;
    }

    public IRedisManager getRedisManager() {
        return redisManager;
    }

    public void setRedisManager(IRedisManager redisManager) {
        this.redisManager = redisManager;
    }

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public void setKeyPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
    }

    public RedisSerializer<?> getKeySerializer() {
        return keySerializer;
    }

    public void setKeySerializer(RedisSerializer<?> keySerializer) {
        this.keySerializer = keySerializer;
    }

    public RedisSerializer<?> getValueSerializer() {
        return valueSerializer;
    }

    public void setValueSerializer(RedisSerializer<?> valueSerializer) {
        this.valueSerializer = valueSerializer;
    }

    public long getSessionInMemoryTimeout() {
        return sessionInMemoryTimeout;
    }

    public void setSessionInMemoryTimeout(long sessionInMemoryTimeout) {
        this.sessionInMemoryTimeout = sessionInMemoryTimeout;
    }

    public int getExpire() {
        return expire;
    }

    public void setExpire(int expire) {
        this.expire = expire;
    }

    public boolean getSessionInMemoryEnabled() {
        return sessionInMemoryEnabled;
    }

    public void setSessionInMemoryEnabled(boolean sessionInMemoryEnabled) {
        this.sessionInMemoryEnabled = sessionInMemoryEnabled;
    }

    public static ThreadLocal<Cache<Serializable, SessionInMemory>> getSessionsInThread() {
        return SESSIONS_IN_THREAD;
    }
}
