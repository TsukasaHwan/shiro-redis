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
