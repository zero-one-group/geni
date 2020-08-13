package zero_one.geni.rdd.function;

import clojure.lang.IFn;
import scala.Tuple2;

public class PairFunction extends zero_one.geni.rdd.serialization.AbstractSerializableWrappedIFn implements org.apache.spark.api.java.function.PairFunction {
    public PairFunction(IFn func) {
        super(func);
    }

    @SuppressWarnings("unchecked")
  public Tuple2<Object, Object> call(Object v1) throws Exception {
    return (Tuple2<Object, Object>) f.invoke(v1);
  }
}
