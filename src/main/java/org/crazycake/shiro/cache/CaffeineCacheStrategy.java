package org.crazycake.shiro.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.shiro.session.Session;
import org.crazycake.shiro.common.SessionInMemory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * CaffeineCacheStrategy
 *
 * @author Teamo
 * @since 2025/3/30
 */
public class CaffeineCacheStrategy implements CacheStrategy {

    private final static Logger logger = LoggerFactory.getLogger(CaffeineCacheStrategy.class);

    private final ThreadLocal<Cache<Serializable, SessionInMemory>> sessionsInThread = new ThreadLocal<>();

    private long sessionInMemoryTimeout;

    public CaffeineCacheStrategy(long sessionInMemoryTimeout) {
        this.sessionInMemoryTimeout = sessionInMemoryTimeout;
    }

    @Override
    public void put(Serializable sessionId, Session session) {
        getCache().put(sessionId, createSessionInMemory(session));
    }

    @Override
    public Session get(Serializable sessionId) {
        SessionInMemory sessionInMemory = getCache().getIfPresent(sessionId);
        if (sessionInMemory == null) {
            return null;
        }
        logger.debug("read session from caffeine cache");
        return sessionInMemory.getSession();
    }

    @Override
    public void remove(Serializable sessionId) {
        getCache().invalidate(sessionId);
    }

    @Override
    public void removeExpired() {
        Cache<Serializable, SessionInMemory> sessionCache = sessionsInThread.get();
        if (sessionCache == null) {
            return;
        }

        sessionCache.cleanUp();

        if (sessionCache.asMap().isEmpty()) {
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

    private Cache<Serializable, SessionInMemory> getCache() {
        Cache<Serializable, SessionInMemory> cache = sessionsInThread.get();
        if (cache == null) {
            cache = Caffeine.newBuilder()
                    .expireAfterWrite(this.sessionInMemoryTimeout, TimeUnit.MILLISECONDS)
                    .build();
            sessionsInThread.set(cache);
        }
        return cache;
    }

    private SessionInMemory createSessionInMemory(Session session) {
        SessionInMemory s = new SessionInMemory();
        s.setCreateTime(new Date());
        s.setSession(session);
        return s;
    }
}
