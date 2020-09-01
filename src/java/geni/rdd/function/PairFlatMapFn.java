package zero_one.geni.rdd.function;


import clojure.lang.IFn;

import java.util.Collection;
import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;


/**
 * Compatibility wrapper for a Spark `PairFlatMapFunction` of one argument
 * which returns a sequence of pairs.
 */
public class PairFlatMapFn extends SerializableFn implements PairFlatMapFunction {

    public PairFlatMapFn(IFn f, Collection<String> namespaces) {
        super(f, namespaces);
    }


    @Override
    @SuppressWarnings("unchecked")
    public Iterator<Tuple2<Object, Object>> call(Object v1) throws Exception {
        Collection<Object> result = (Collection<Object>)f.invoke(v1);
        Iterator<Object> results = result.iterator();
        return new Iterator<Tuple2<Object, Object>>() {
            public boolean hasNext() {
                return results.hasNext();
            }

            public Tuple2<Object, Object> next() {
                return PairFn.coercePair(f, results.next());
            }
        };
    }

}
