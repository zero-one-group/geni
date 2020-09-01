package zero_one.geni.rdd.function;


import clojure.lang.IFn;

import java.util.Collection;

import org.apache.spark.api.java.function.Function;


/**
 * Compatibility wrapper for a Spark `Function` of one argument.
 */
public class Fn1 extends SerializableFn implements Function {

    public Fn1(IFn f, Collection<String> namespaces) {
        super(f, namespaces);
    }


    @Override
    @SuppressWarnings("unchecked")
    public Object call(Object v1) throws Exception {
        return f.invoke(v1);
    }

}
