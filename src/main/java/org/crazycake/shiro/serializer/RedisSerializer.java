package org.crazycake.shiro.serializer;

import org.crazycake.shiro.exception.SerializationException;

/**
 * @author Alexy Yang
 */
public interface RedisSerializer<T> {

    /**
     * serialize
     *
     * @param t object
     * @return byte array
     * @throws SerializationException SerializationException
     */
    byte[] serialize(T t) throws SerializationException;

    /**
     * deserialize
     *
     * @param bytes byte array
     * @return object
     * @throws SerializationException SerializationException
     */
    T deserialize(byte[] bytes) throws SerializationException;
}
