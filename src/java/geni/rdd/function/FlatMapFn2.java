// Taken from https://github.com/amperity/sparkplug
package zero_one.geni.rdd.function;


import clojure.lang.IFn;

import java.util.Iterator;
import java.util.Collection;

import org.apache.spark.api.java.function.FlatMapFunction2;


/**
 * Compatibility wrapper for a Spark `FlatMapFunction2` of two arguments.
 */
public class FlatMapFn2 extends SerializableFn implements FlatMapFunction2 {

    public FlatMapFn2(IFn f, Collection<String> namespaces) {
        super(f, namespaces);
    }


    @Override
    @SuppressWarnings("unchecked")
    public Iterator<Object> call(Object v1, Object v2) throws Exception {
        Collection<Object> results = (Collection<Object>)f.invoke(v1, v2);
        return results.iterator();
    }

}
