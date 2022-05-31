package org.crazycake.shiro.serializer;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.crazycake.shiro.exception.SerializationException;

/**
 * @author Teamo
 * @date 2022/05/31
 */
public class ProtostuffSerializer implements RedisSerializer<Object> {
    /**
     * Avoid re-applying Buffer space for each serialization
     */
    private static final LinkedBuffer BUFFER = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);

    /**
     * Serialize/deserialize wrapper class Class objects
     */
    private static final Class<SerializeDeserializeWrapper> WRAPPER_CLASS = SerializeDeserializeWrapper.class;

    /**
     * Serialize/deserialize wrapper class Schema objects
     */
    private static final Schema<SerializeDeserializeWrapper> WRAPPER_SCHEMA = RuntimeSchema.createFrom(WRAPPER_CLASS);

    @Override
    public byte[] serialize(Object o) throws SerializationException {
        SerializeDeserializeWrapper<Object> builder = SerializeDeserializeWrapper.builder(o);
        byte[] data;
        try {
            data = ProtostuffIOUtil.toByteArray(builder, WRAPPER_SCHEMA, BUFFER);
        } finally {
            BUFFER.clear();
        }
        return data;
    }

    @Override
    public Object deserialize(byte[] bytes) throws SerializationException {
        SerializeDeserializeWrapper<Object> wrapper = new SerializeDeserializeWrapper<>();
        ProtostuffIOUtil.mergeFrom(bytes, wrapper, WRAPPER_SCHEMA);
        return wrapper.getData();
    }

    /**
     * <p>
     * Serialize/deserialize object wrapper class
     * Specifically defined for serialization/deserialization based on Protostuff.
     * Protostuff is based on POJO serialization and deserialization operations.
     * If the object to be serialized/deserialized does not know its type, serialization/deserialization cannot be performed;
     * For example, Map, List, String, Enum, etc. cannot be serialized/deserialized correctly.
     * Therefore, it needs to be mapped into a wrapper class, and put these objects that need to be serialized/deserialized into this wrapper class.
     * In this way, every time Protostuff serializes/deserializes this class, there will be no inability/abnormal operation.
     * </p>
     *
     * @author Teamo
     * @date 2022/05/31
     */
    public static class SerializeDeserializeWrapper<T> {
        private T data;

        public static <T> SerializeDeserializeWrapper<T> builder(T data) {
            SerializeDeserializeWrapper<T> wrapper = new SerializeDeserializeWrapper<>();
            wrapper.setData(data);
            return wrapper;
        }

        public T getData() {
            return data;
        }

        public void setData(T data) {
            this.data = data;
        }
    }
}
