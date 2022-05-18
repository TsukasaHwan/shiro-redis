package org.crazycake.shiro.exception;

/**
 * @author Teamo
 * @date 2022/05/18
 */
public class LettucePoolException extends RuntimeException {
    /**
     * Constructs a new <code>LettucePoolException</code> instance.
     *
     * @param msg the detail message.
     */
    public LettucePoolException(String msg) {
        super(msg);
    }

    /**
     * Constructs a new <code>LettucePoolException</code> instance.
     *
     * @param msg   the detail message.
     * @param cause the nested exception.
     */
    public LettucePoolException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
