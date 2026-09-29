// Taken from https://github.com/amperity/sparkplug
package zero_one.geni.rdd.function;


import clojure.lang.Compiler;
import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base class for function classes built for interop with Spark and Scala.
 *
 * This class is designed to be serialized across computation boundaries in a
 * manner compatible with Spark and Kryo, while ensuring that required code is
 * loaded upon deserialization.
 */
public abstract class SerializableFn implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(SerializableFn.class);
    private static final Var require = RT.var("clojure.core", "require");

    protected IFn f;
    protected List<String> namespaces;


    /**
     * Default empty constructor.
     */
    private SerializableFn() {
    }


    /**
     * Construct a new serializable wrapper for the function with an explicit
     * set of required namespaces.
     *
     * @param fn Clojure function to wrap
     * @param namespaces collection of namespaces required
     */
    protected SerializableFn(IFn fn, Collection<String> namespaces) {
        this.f = fn;
        List<String> namespaceColl = new ArrayList<String>(namespaces);
        Collections.sort(namespaceColl);
        this.namespaces = Collections.unmodifiableList(namespaceColl);
    }


    /**
     * Serialize the function to the provided output stream.
     * An unspoken part of the `Serializable` interface.
     *
     * @param out stream to write the function to
     */
    private void writeObject(ObjectOutputStream out) throws IOException {
        try {
            logger.trace("Serializing " + f);
            // Write the function class name
            // This is only used for debugging
            out.writeObject(f.getClass().getName());
            // Write out the referenced namespaces.
            out.writeInt(namespaces.size());
            for (String ns : namespaces) {
                out.writeObject(ns);
            }
            // Write out the function itself.
            out.writeObject(f);
        } catch (IOException ex) {
            logger.error("Error serializing function " + f, ex);
            throw ex;
        } catch (RuntimeException ex){
            logger.error("Error serializing function " + f, ex);
            throw ex;
        }
    }


    /**
     * Deserialize a function from the provided input stream.
     * An unspoken part of the `Serializable` interface.
     *
     * @param in stream to read the function from
     */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        String className = "";
        try {
            // Read the function class name.
            className = (String)in.readObject();
            logger.trace("Deserializing " + className);
            // Read the referenced namespaces and load them.
            int nsCount = in.readInt();
            this.namespaces = new ArrayList<String>(nsCount);
            for (int i = 0; i < nsCount; i++) {
                String ns = (String)in.readObject();
                namespaces.add(ns);
                requireNamespace(ns);
            }
            // Read the function itself.
            this.f = (IFn)in.readObject();
        } catch (IOException ex) {
            logger.error("IO error deserializing function " + className, ex);
            throw ex;
        } catch (ClassNotFoundException ex) {
            logger.error("Class error deserializing function " + className, ex);
            throw ex;
        } catch (RuntimeException ex) {
            logger.error("Error deserializing function " + className, ex);
            throw ex;
        }
    }


    /**
     * Load the namespace specified by the given symbol.
     *
     * @param namespace string designating the namespace to load
     */
    private static void requireNamespace(String namespace) {
        try {
            logger.trace("(require " + namespace + ")");
            synchronized (RT.REQUIRE_LOCK) {
                Symbol sym = Symbol.intern(namespace);
                require.invoke(sym);
            }
        } catch (Exception ex) {
            logger.warn("Error loading namespace " + namespace, ex);
        }
    }

}
