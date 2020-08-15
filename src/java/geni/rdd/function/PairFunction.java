package zero_one.geni.rdd.function;

import clojure.lang.IFn;
import clojure.lang.IMapEntry;
import clojure.lang.IPersistentVector;
import scala.Tuple2;

public class PairFunction extends zero_one.geni.rdd.serialization.AbstractSerializableWrappedIFn implements org.apache.spark.api.java.function.PairFunction {
    public PairFunction(IFn func) {
        super(func);
    }

    @SuppressWarnings("unchecked")
    public Tuple2<Object, Object> call(Object v1) throws Exception {
        return coercePair(f, f.invoke(v1));
    }

    /**
    * Coerce a result value into a Scala `Tuple2` as the result of a function.
    *
    * @param f the function which produced the result, to report in error messages
    * @param result object to try to coerce
    * @return a Scala tuple with two values
    */
    public static Tuple2<Object, Object> coercePair(IFn f, Object result) {
        if (result == null) {
            throw new RuntimeException("Wrapped pair function " + f + " returned a null");
        } else if (result instanceof Tuple2) {
            return (Tuple2<Object, Object>)result;
        } else if (result instanceof IMapEntry) {
            IMapEntry entry = (IMapEntry)result;
            return new Tuple2(entry.key(), entry.val());
        } else if (result instanceof IPersistentVector) {
            IPersistentVector vector = (IPersistentVector)result;
            if (vector.count() != 2) {
                throw new RuntimeException("Wrapped pair function " + f + " returned a vector without exactly two values: " + vector.count());
            }
            return new Tuple2(vector.nth(0), vector.nth(1));
        } else {
            throw new RuntimeException("Wrapped pair function " + f + " returned an invalid pair type: " + result.getClass().getName());
        }
    }
}
