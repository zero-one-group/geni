package zero_one.geni.rdd.function;


import clojure.lang.IFn;

import java.util.Collection;

import org.apache.spark.api.java.function.Function2;


/**
 * Compatibility wrapper for a Spark `Function2` of two arguments.
 */
public class Fn2 extends SerializableFn implements Function2 {

    public Fn2(IFn f, Collection<String> namespaces) {
        super(f, namespaces);
    }


    @Override
    @SuppressWarnings("unchecked")
    public Object call(Object v1, Object v2) throws Exception {
        return f.invoke(v1, v2);
    }

}
