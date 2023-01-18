package org.crazycake.shiro.exception;

/**
 * @author Alexy Yang
 */
public class CacheManagerPrincipalIdNotAssignedException extends RuntimeException {

    private static final String MESSAGE = "CacheManager didn't assign Principal Id field name!";

    public CacheManagerPrincipalIdNotAssignedException() {
        super(MESSAGE);
    }
}
