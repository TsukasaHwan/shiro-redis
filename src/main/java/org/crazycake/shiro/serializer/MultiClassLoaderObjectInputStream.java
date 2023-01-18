package org.crazycake.shiro.serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

/**
 * For fixing <a href="https://github.com/alexxiyang/shiro-redis/issues/84"/>
 *
 * @author Alexy Yang
 */
public class MultiClassLoaderObjectInputStream extends ObjectInputStream {

    MultiClassLoaderObjectInputStream(InputStream is) throws IOException {
        super(is);
    }

    /**
     * Try :
     * 1. thread class loader
     * 2. application class loader
     * 3. system class loader
     *
     * @param desc
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     */
    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
        String name = desc.getName();

        try {
            return super.resolveClass(desc);
        } catch (ClassNotFoundException ex) {
            try {
                return Class.forName(name, false, Thread.currentThread().getContextClassLoader());
            } catch (ClassNotFoundException e) {
                try {
                    return Class.forName(name, false, MultiClassLoaderObjectInputStream.class.getClassLoader());
                } catch (ClassNotFoundException exc) {
                    return Class.forName(name, false, ClassLoader.getSystemClassLoader());
                }
            }
        }
    }

}
