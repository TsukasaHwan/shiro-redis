package org.crazycake.shiro.common;

import org.apache.shiro.session.Session;

import java.time.LocalDateTime;

/**
 * Use ThreadLocal as a temporary storage of Session, so that shiro wouldn't keep read redis several times while a request coming.
 *
 * @author AlexYang
 */
public class SessionInMemory {
    private Session session;

    private LocalDateTime createTime;

    public SessionInMemory(Session session) {
        this.session = session;
        this.createTime = LocalDateTime.now();
    }

    public Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public LocalDateTime getCreateTime() {
        return createTime;
    }

    public void setCreateTime(LocalDateTime createTime) {
        this.createTime = createTime;
    }
}
