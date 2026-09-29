// Taken from https://github.com/amperity/sparkplug
package zero_one.geni.rdd.function;


import clojure.lang.IFn;

import java.util.Iterator;
import java.util.Collection;

import org.apache.spark.api.java.function.FlatMapFunction;


/**
 * Compatibility wrapper for a Spark `FlatMapFunction` of one argument.
 */
public class FlatMapFn1 extends SerializableFn implements FlatMapFunction {

    public FlatMapFn1(IFn f, Collection<String> namespaces) {
        super(f, namespaces);
    }


    @Override
    @SuppressWarnings("unchecked")
    public Iterator<Object> call(Object v1) throws Exception {
        Collection<Object> results = (Collection<Object>)f.invoke(v1);
        return results.iterator();
    }

}
