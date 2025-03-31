package org.crazycake.shiro;

import org.apache.shiro.session.InvalidSessionException;
import org.apache.shiro.session.mgt.SimpleSession;
import org.crazycake.shiro.cache.CacheStrategy;
import org.crazycake.shiro.cache.CaffeineCacheStrategy;
import org.crazycake.shiro.cache.MapCacheStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.mockito.Mockito.mock;

/**
 * @author Teamo
 * @since 2025/3/31
 */
public class CacheStrategyTest {

    private IRedisManager redisManager;

    @BeforeEach
    public void setUp() {
        redisManager = mock(IRedisManager.class);
    }

    @Test
    public void testCaffeinePerformance() throws InterruptedException {
        RedisSessionDAO caffeineDAO = mountRedisSessionDAOWithCacheStrategy(new CaffeineCacheStrategy(), 3000);
        measurePerformance(caffeineDAO, "CaffeineCache");
    }

    @Test
    public void testMapPerformance() throws InterruptedException {
        RedisSessionDAO mapDAO = mountRedisSessionDAOWithCacheStrategy(new MapCacheStrategy(), 3000);
        measurePerformance(mapDAO, "MapCache");
    }

    private RedisSessionDAO mountRedisSessionDAOWithCacheStrategy(
            CacheStrategy cacheStrategy,
            Integer expire) {
        RedisSessionDAO redisSessionDAO = new RedisSessionDAO();
        redisSessionDAO.setExpire(expire);
        redisSessionDAO.setKeyPrefix("student:");
        redisSessionDAO.setRedisManager(redisManager);
        redisSessionDAO.setCacheStrategy(cacheStrategy);
        return redisSessionDAO;
    }

    private void measurePerformance(RedisSessionDAO sessionDAO, String cacheType) throws InterruptedException {
        Runtime runtime = Runtime.getRuntime();
        int threadsSize = 200;
        CountDownLatch cdl = new CountDownLatch(threadsSize);
        Runnable runnable = () -> {
            // 调整循环次数以放大性能差异
            int loopCount = 15;
            StudentSession session = null;
            for (int i = 0; i < loopCount; i++) {
                session = new StudentSession(i, 2000);

                sessionDAO.update(session);

                sessionDAO.doReadSession(session.getId());
            }
            cdl.countDown();
        };
        List<Thread> threads = new ArrayList<>();
        Thread thread = null;
        for (int i = 0; i < threadsSize; i++) {
            thread = new Thread(runnable);
            threads.add(thread);
        }

        System.gc();
        Thread.sleep(100);

        long initialUsedMemory = runtime.totalMemory() - runtime.freeMemory();
        System.out.println(cacheType + " initial used memory: " + initialUsedMemory / 1024 / 1024 + "MB");
        long startTime = System.currentTimeMillis();

        threads.parallelStream().forEach(Thread::start);

        cdl.await();

        long duration = System.currentTimeMillis() - startTime;
        long finalUsedMemory = runtime.totalMemory() - runtime.freeMemory();
        System.out.println(cacheType + " total used time: " + duration + "ms");
        System.out.println(cacheType + " final used memory: " + ((finalUsedMemory / 1024 / 1024) - (initialUsedMemory / 1024 / 1024)) + "MB");
    }

    static class StudentSession extends SimpleSession {
        private final Integer id;
        private final long timeout;

        public StudentSession(Integer id, long timeout) {
            this.id = id;
            this.timeout = timeout;
        }

        @Override
        public Serializable getId() {
            return id;
        }

        @Override
        public Date getStartTimestamp() {
            return null;
        }

        @Override
        public Date getLastAccessTime() {
            return null;
        }

        @Override
        public long getTimeout() throws InvalidSessionException {
            return timeout;
        }

        @Override
        public void setTimeout(long l) throws InvalidSessionException {

        }

        @Override
        public String getHost() {
            return null;
        }

        @Override
        public void touch() throws InvalidSessionException {

        }

        @Override
        public void stop() throws InvalidSessionException {

        }

        @Override
        public Collection<Object> getAttributeKeys() throws InvalidSessionException {
            return null;
        }

        @Override
        public Object getAttribute(Object o) throws InvalidSessionException {
            return null;
        }

        @Override
        public void setAttribute(Object o, Object o1) throws InvalidSessionException {

        }

        @Override
        public Object removeAttribute(Object o) throws InvalidSessionException {
            return null;
        }
    }
}
