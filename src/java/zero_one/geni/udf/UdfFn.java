package zero_one.geni.udf;


import clojure.lang.ArraySeq;
import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Var;

import java.util.Collection;

import org.apache.spark.sql.api.java.*;
import org.apache.spark.sql.types.DataType;

import zero_one.geni.rdd.function.SerializableFn;


/**
 * A Clojure function as a Spark SQL UDF of 0 to 10 arguments, as many as
 * Spark's `functions.udf` takes, for `g/udf`.
 *
 * Spark's values go through `zero-one.geni.interop/->clojure` on their way in,
 * and the function's result is converted for the declared return type on its
 * way out, by a converter that `zero-one.geni.core.udf/result-converter` builds
 * from that type the first time it's needed. So only the function and the type
 * are serialised, and the executors require the namespaces the function uses,
 * as `SerializableFn` does.
 */
@SuppressWarnings("rawtypes")
public class UdfFn extends SerializableFn
    implements UDF0, UDF1, UDF2, UDF3, UDF4, UDF5, UDF6, UDF7, UDF8, UDF9, UDF10 {

    private static final long serialVersionUID = 1L;

    private static final Var toClojure = RT.var("zero-one.geni.interop", "->clojure");
    private static final Var resultConverter =
        RT.var("zero-one.geni.core.udf", "result-converter");

    private final DataType returnType;
    private transient IFn converter;


    public UdfFn(IFn f, DataType returnType, Collection<String> namespaces) {
        super(f, namespaces);
        this.returnType = returnType;
    }


    private Object apply(Object[] args) {
        for (int i = 0; i < args.length; i++) {
            args[i] = toClojure.invoke(args[i]);
        }
        if (converter == null) {
            converter = (IFn) resultConverter.invoke(returnType);
        }
        return converter.invoke(f.applyTo(ArraySeq.create(args)));
    }


    @Override
    public Object call() throws Exception {
        return apply(new Object[] {});
    }

    @Override
    public Object call(Object v1) throws Exception {
        return apply(new Object[] {v1});
    }

    @Override
    public Object call(Object v1, Object v2) throws Exception {
        return apply(new Object[] {v1, v2});
    }

    @Override
    public Object call(Object v1, Object v2, Object v3) throws Exception {
        return apply(new Object[] {v1, v2, v3});
    }

    @Override
    public Object call(Object v1, Object v2, Object v3, Object v4) throws Exception {
        return apply(new Object[] {v1, v2, v3, v4});
    }

    @Override
    public Object call(Object v1, Object v2, Object v3, Object v4, Object v5)
                       throws Exception {
        return apply(new Object[] {v1, v2, v3, v4, v5});
    }

    @Override
    public Object call(Object v1, Object v2, Object v3, Object v4, Object v5, Object v6)
                       throws Exception {
        return apply(new Object[] {v1, v2, v3, v4, v5, v6});
    }

    @Override
    public Object call(Object v1, Object v2, Object v3, Object v4, Object v5, Object v6,
                       Object v7) throws Exception {
        return apply(new Object[] {v1, v2, v3, v4, v5, v6, v7});
    }

    @Override
    public Object call(Object v1, Object v2, Object v3, Object v4, Object v5, Object v6,
                       Object v7, Object v8) throws Exception {
        return apply(new Object[] {v1, v2, v3, v4, v5, v6, v7, v8});
    }

    @Override
    public Object call(Object v1, Object v2, Object v3, Object v4, Object v5, Object v6,
                       Object v7, Object v8, Object v9) throws Exception {
        return apply(new Object[] {v1, v2, v3, v4, v5, v6, v7, v8, v9});
    }

    @Override
    public Object call(Object v1, Object v2, Object v3, Object v4, Object v5, Object v6,
                       Object v7, Object v8, Object v9, Object v10) throws Exception {
        return apply(new Object[] {v1, v2, v3, v4, v5, v6, v7, v8, v9, v10});
    }
}
