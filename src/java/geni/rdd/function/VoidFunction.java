package zero_one.geni.rdd.function;

import clojure.lang.IFn;

public class VoidFunction extends zero_one.geni.rdd.serialization.AbstractSerializableWrappedIFn implements org.apache.spark.api.java.function.VoidFunction {
  
  @SuppressWarnings("unchecked")
  public void call(Object v1) throws Exception {
    f.invoke(v1);
  }

    public VoidFunction(IFn func) {
        super(func);
    }
}
