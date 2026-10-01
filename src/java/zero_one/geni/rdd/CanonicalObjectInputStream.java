package zero_one.geni.rdd;


import clojure.lang.RT;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.lang.reflect.Proxy;


/**
 * An ObjectInputStream that reads booleans back as Boolean.TRUE and
 * Boolean.FALSE.
 *
 * Java's deserialisation makes a new Boolean for each boolean it reads, and
 * Clojure treats every Boolean but Boolean.FALSE as true, so a false would
 * turn true, wherever it sits in what's read: a field, a map or a vector.
 *
 * It resolves classes through the given class loader, or else through the
 * thread's context class loader, which Spark sets to the task's, and through
 * Clojure's, which also knows the classes compiled at run time.
 */
public class CanonicalObjectInputStream extends ObjectInputStream {

    private final ClassLoader loader;


    /**
     * @param in the stream to read
     * @param loader the class loader to resolve classes with, or null for the
     *        thread's context class loader at the time
     */
    public CanonicalObjectInputStream(InputStream in, ClassLoader loader) throws IOException {
        super(in);
        this.loader = loader;
        enableResolveObject(true);
    }


    private ClassLoader loader() {
        if (loader != null) {
            return loader;
        }
        ClassLoader context = Thread.currentThread().getContextClassLoader();
        return context != null ? context : RT.baseLoader();
    }


    @Override
    protected Object resolveObject(Object obj) {
        return (obj instanceof Boolean) ? Boolean.valueOf((Boolean)obj) : obj;
    }


    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc)
        throws IOException, ClassNotFoundException {
        try {
            return RT.classForName(desc.getName(), false, loader());
        } catch (Exception ex) {
            // Java's own lookup, which also knows the primitive types.
            // RT.classForName throws ClassNotFoundException undeclared.
            return super.resolveClass(desc);
        }
    }


    @Override
    @SuppressWarnings("deprecation")
    protected Class<?> resolveProxyClass(String[] interfaces)
        throws IOException, ClassNotFoundException {
        // As Spark's JavaSerializer does.
        ClassLoader classLoader = loader();
        Class<?>[] resolved = new Class<?>[interfaces.length];
        for (int i = 0; i < interfaces.length; i++) {
            resolved[i] = Class.forName(interfaces[i], false, classLoader);
        }
        return Proxy.getProxyClass(classLoader, resolved);
    }

}
