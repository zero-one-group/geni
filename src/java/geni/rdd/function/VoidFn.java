// Taken from https://github.com/amperity/sparkplug
package zero_one.geni.rdd.function;


import clojure.lang.IFn;

import java.util.Collection;

import org.apache.spark.api.java.function.VoidFunction;


/**
 * Compatibility wrapper for a Spark `VoidFunction` of one argument.
 */
public class VoidFn extends SerializableFn implements VoidFunction {

    public VoidFn(IFn f, Collection<String> namespaces) {
        super(f, namespaces);
    }


    @Override
    @SuppressWarnings("unchecked")
    public void call(Object v1) throws Exception {
        f.invoke(v1);
    }

}
