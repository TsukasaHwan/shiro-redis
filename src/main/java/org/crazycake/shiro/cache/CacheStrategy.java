package org.crazycake.shiro.cache;

import org.apache.shiro.session.Session;

import java.io.Serializable;

/**
 * Cache strategy
 *
 * @author Teamo
 * @since 2025/3/30
 */
public interface CacheStrategy {

    /**
     * doReadSession be called about 10 times when login.
     * Save Session in ThreadLocal to resolve this problem. sessionInMemoryTimeout is expiration of Session in ThreadLocal.
     * The default value is 1000 milliseconds (1s).
     * Most of time, you don't need to change it.
     * <p>
     * You can turn it off by setting sessionInMemoryEnabled to false
     */
    long DEFAULT_SESSION_IN_MEMORY_TIMEOUT = 1000L;

    /**
     * Put session into cache
     *
     * @param sessionId session id
     * @param session   session
     */
    void put(Serializable sessionId, Session session);

    /**
     * Get session from cache
     *
     * @param sessionId session id
     * @return session
     */
    Session get(Serializable sessionId);

    /**
     * Remove session from cache
     *
     * @param sessionId session id
     */
    void remove(Serializable sessionId);

    /**
     * Remove expired session from cache
     */
    void removeExpired();

    /**
     * Set session in memory timeout
     *
     * @param sessionInMemoryTimeout session in memory timeout
     */
    void setSessionInMemoryTimeout(long sessionInMemoryTimeout);

    /**
     * Get session in memory timeout
     *
     * @return session in memory timeout
     */
    long getSessionInMemoryTimeout();
}
