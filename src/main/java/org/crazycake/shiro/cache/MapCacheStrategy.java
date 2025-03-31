package org.crazycake.shiro.cache;

import org.apache.shiro.session.Session;
import org.crazycake.shiro.common.SessionInMemory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * MapCacheStrategy
 *
 * @author Teamo
 * @since 2025/3/30
 */
public class MapCacheStrategy implements CacheStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapCacheStrategy.class);

    private final ThreadLocal<Map<Serializable, SessionInMemory>> sessionsInThread = ThreadLocal.withInitial(HashMap::new);

    private long sessionInMemoryTimeout = DEFAULT_SESSION_IN_MEMORY_TIMEOUT;

    @Override
    public void put(Serializable sessionId, Session session) {
        sessionsInThread.get().put(sessionId, createSessionInMemory(session));
    }

    @Override
    public Session get(Serializable sessionId) {
        SessionInMemory sessionInMemory = sessionsInThread.get().get(sessionId);
        if (sessionInMemory == null) {
            return null;
        }
        LOGGER.debug("read session from map cache");
        return sessionInMemory.getSession();
    }

    @Override
    public void remove(Serializable sessionId) {
        sessionsInThread.get().remove(sessionId);
    }

    @Override
    public void removeExpired() {
        Map<Serializable, SessionInMemory> sessionMap = sessionsInThread.get();
        if (sessionMap == null) {
            return;
        }

        sessionMap.keySet().removeIf(id -> {
            SessionInMemory sessionInMemory = sessionMap.get(id);
            return sessionInMemory == null || getSessionInMemoryLiveTime(sessionInMemory) > sessionInMemoryTimeout;
        });

        if (sessionMap.isEmpty()) {
            sessionsInThread.remove();
        }
    }

    @Override
    public void setSessionInMemoryTimeout(long sessionInMemoryTimeout) {
        this.sessionInMemoryTimeout = sessionInMemoryTimeout;
    }

    @Override
    public long getSessionInMemoryTimeout() {
        return this.sessionInMemoryTimeout;
    }

    private SessionInMemory createSessionInMemory(Session session) {
        SessionInMemory sessionInMemory = new SessionInMemory();
        sessionInMemory.setCreateTime(new Date());
        sessionInMemory.setSession(session);
        return sessionInMemory;
    }

    private long getSessionInMemoryLiveTime(SessionInMemory sessionInMemory) {
        return Instant.now().toEpochMilli() - sessionInMemory.getCreateTime().getTime();
    }
}
