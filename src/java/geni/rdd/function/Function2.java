package zero_one.geni.rdd.function;

import clojure.lang.IFn;

public class Function2 extends zero_one.geni.rdd.serialization.AbstractSerializableWrappedIFn implements org.apache.spark.api.java.function.Function2, org.apache.spark.sql.api.java.UDF2 {
    public Function2(IFn func) {
        super(func);
    }

    public Object call(Object v1, Object v2) throws Exception {
    return f.invoke(v1, v2);
  }
}
