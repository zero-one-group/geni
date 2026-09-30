(ns zero-one.geni.core.udf
  "Spark SQL UDFs from Clojure functions, on classic Spark."
  (:require
   [zero-one.geni.core.column :refer [->col-array]]
   [zero-one.geni.core.dataset-creation :as dataset-creation]
   [zero-one.geni.defaults :as defaults]
   [zero-one.geni.spark :as spark])
  (:import
   (java.lang.reflect Method)
   (java.util ArrayList HashMap)
   (org.apache.spark.sql Row RowFactory SparkSession functions)
   (org.apache.spark.sql.expressions UserDefinedFunction)
   (org.apache.spark.sql.types ArrayType ByteType DataType DecimalType DoubleType FloatType
                               IntegerType LongType MapType ShortType StringType StructField
                               StructType)
   (zero_one.geni.udf UdfFn)))

(def ^:private max-arity
  "The most columns that Spark's `functions.udf` takes."
  10)

(defn- ->string [value]
  (cond
    (string? value)  value
    (keyword? value) (name value)
    :else            (str value)))

(defn- struct-converter [^StructType data-type field-converter]
  (let [fields (.fields data-type)
        names  (mapv #(.name ^StructField %) fields)
        ks     (mapv keyword names)
        convs  (mapv #(field-converter (.dataType ^StructField %)) fields)]
    (fn [value]
      (cond
        (instance? Row value) value
        (map? value)          (RowFactory/create
                               (object-array
                                (map (fn [k n convert]
                                       (convert (if (contains? value k) (get value k) (get value n))))
                                     ks names convs)))
        :else                 (RowFactory/create (object-array (map #(%1 %2) convs value)))))))

(defn ^:no-doc result-converter
  "Returns a function that turns a UDF's result into the Java value that Spark
  expects for `data-type`: a number of the declared width, a string for a
  keyword, a list for an array, a map for a map, and a Row for a struct, from
  a map with keyword or string keys, or from the values in order. `nil` stays
  nil, and other values go to Spark as they are. `zero_one.geni.udf.UdfFn`
  calls it on each executor, the first time the UDF runs there."
  [^DataType data-type]
  (let [convert (condp instance? data-type
                  IntegerType int
                  LongType    long
                  ShortType   short
                  ByteType    byte
                  DoubleType  double
                  FloatType   float
                  DecimalType bigdec
                  StringType  ->string
                  ArrayType   (let [element (result-converter (.elementType ^ArrayType data-type))]
                                (fn [values]
                                  (let [out (ArrayList.)]
                                    (doseq [value values]
                                      (.add out (element value)))
                                    out)))
                  MapType     (let [k (result-converter (.keyType ^MapType data-type))
                                    v (result-converter (.valueType ^MapType data-type))]
                                (fn [m]
                                  (let [out (HashMap.)]
                                    (doseq [[mk mv] m]
                                      (.put out (k mk) (v mv)))
                                    out)))
                  StructType  (struct-converter data-type result-converter)
                  identity)]
    (fn [value]
      (when (some? value)
        (convert value)))))

(defn- ->data-type
  "The Spark type for a UDF's return type."
  ^DataType [return-type]
  (let [data-type (cond
                    (instance? DataType return-type) return-type
                    (keyword? return-type)           (dataset-creation/data-type->spark-type return-type)
                    (or (vector? return-type)
                        (map? return-type))          (dataset-creation/->schema return-type))]
    (if (instance? DataType data-type)
      data-type
      (throw (ex-info (str "Unknown UDF return type " (pr-str return-type) ". Pass a type keyword "
                           "such as :long, a schema such as [:string] or {:a :int}, or a Spark "
                           "DataType.")
                      {:return-type return-type})))))

(defn- classic-only! []
  (when (spark/connect-only?)
    (throw (ex-info (str "UDFs need classic Spark: a Clojure UDF runs on the executors, and a "
                         "Spark Connect server can't load it.")
                    {}))))

(defn- ->udf-fn
  "Wraps `f` for Spark, with the namespaces that the executors need to load
  to run it."
  ^UdfFn [f ^DataType data-type]
  (let [namespace-references (requiring-resolve 'zero-one.geni.rdd.function/namespace-references)
        namespaces           (into #{"zero-one.geni.core.udf" "zero-one.geni.interop"}
                                   (map str)
                                   (namespace-references f))]
    (UdfFn. f data-type namespaces)))

(defmacro ^:private spark-udf-of-arity
  "Spark's `functions.udf` for the UDF interface of `n` arguments."
  [n udf-fn data-type]
  `(case (int ~n)
     ~@(mapcat (fn [i]
                 (let [tag (symbol (str "org.apache.spark.sql.api.java.UDF" i))]
                   [i `(functions/udf ~(with-meta udf-fn {:tag tag}) ~data-type)]))
               (range (inc max-arity)))))

(defn- spark-udf
  ^UserDefinedFunction [^UdfFn udf-fn ^DataType data-type n {:keys [deterministic nullable]
                                                             udf-name :name}]
  (when-not (<= 0 n max-arity)
    (throw (ex-info (str "A UDF takes up to " max-arity " columns, as Spark's Java UDFs do, and "
                         "got " n ". Pass more as one g/array or g/struct column.")
                    {:columns n})))
  (let [^UserDefinedFunction u (spark-udf-of-arity n udf-fn data-type)
        ^UserDefinedFunction u (if udf-name (.withName u (name udf-name)) u)
        ^UserDefinedFunction u (if (false? deterministic) (.asNondeterministic u) u)]
    (if (false? nullable) (.asNonNullable u) u)))

(defn- caller
  "A function of columns that calls the UDF with as many arguments as it's
  given columns."
  [udf-for-arity]
  (fn [& exprs]
    (let [^"[Lorg.apache.spark.sql.Column;" cols (->col-array exprs)
          ^UserDefinedFunction u                 (udf-for-arity (alength cols))]
      (.apply u cols))))

(defn udf
  "Returns a function of columns that applies `f` to their values, one row at
  a time, as a Spark UDF, and returns the result as a Column.

  `f` gets each row's values as Clojure data, as `g/collect` gives them: nil
  for a null, a seq for an array, a map for a map or a struct. Its result is
  converted to `return-type`, which is a type keyword such as :long or :string,
  a schema in `g/->schema`'s form, such as [:string] for an array of strings
  or {:a :int} for a struct, or a Spark DataType. So a long becomes an int for
  :int, and a map becomes a struct for {:a :int}.

  The options are:
  - `:name`, which the column's name and `g/explain` show;
  - `:deterministic`, false when `f` can return different results for the same
    values, so that Spark calls it once per row, as it's written;
  - `:nullable`, false when `f` never returns nil.

  UDFs need classic Spark, and run on the executors. Functions defined at a
  REPL, or in a script, work on a local session that Geni starts. On a
  cluster, pass a var, such as `#'my-fn`, which the executors look up in its
  namespace, or AOT-compile the namespace that defines `f`. The Clojure UDFs
  guide has the details.

  ```clojure
  (def plus-one (g/udf inc :long))

  (-> (g/range 3)
      (g/select {:x (plus-one :id)})
      g/collect)
  => ({:x 1} {:x 2} {:x 3})
  ```"
  ([f return-type] (udf f return-type {}))
  ([f return-type opts]
   (classic-only!)
   (let [data-type (->data-type return-type)
         udf-fn    (->udf-fn f data-type)]
     (caller (memoize #(spark-udf udf-fn data-type % opts))))))

(defn- fixed-arity
  "How many arguments `f` takes, when that's one number."
  [f]
  (let [f       (if (var? f) @f f)
        methods (.getDeclaredMethods (class f))
        arities (->> methods
                     (filter #(= "invoke" (.getName ^Method %)))
                     (map #(.getParameterCount ^Method %))
                     distinct)]
    (when (and (= 1 (count arities))
               (not-any? #(= "doInvoke" (.getName ^Method %)) methods))
      (first arities))))

(defn- register! [^SparkSession spark udf-name f return-type {:keys [arity] :as opts}]
  (classic-only!)
  (let [arity     (or arity
                      (fixed-arity f)
                      (throw (ex-info (str "Pass :arity, the number of arguments that SQL calls "
                                           (name udf-name) " with, since the function takes "
                                           "more than one number of them.")
                                      {:udf-name udf-name})))
        data-type (->data-type return-type)
        udf-fn    (->udf-fn f data-type)
        u         (spark-udf udf-fn data-type arity (assoc opts :name udf-name))]
    (.register (.udf spark) (name udf-name) u)
    (caller (fn [n]
              (if (= n arity)
                u
                (throw (ex-info (str (name udf-name) " takes " arity " columns, and got " n ".")
                                {:udf-name udf-name :arity arity :columns n})))))))

(defmulti register-udf!
  "Registers `f` as a Spark UDF called `udf-name` on the session, for
  `g/sql` and `g/expr`, and returns a function of columns that calls it, as
  `g/udf` does. The return type and the options are as for `g/udf`, plus
  `:arity`: the number of columns it takes, for a function that takes more
  than one number of arguments, or any number of them.

  ```clojure
  (g/register-udf! \"plus_one\" inc :long)

  (-> (g/range 3)
      (g/select {:x (g/expr \"plus_one(id)\")})
      g/collect)
  => ({:x 1} {:x 2} {:x 3})
  ```"
  (fn [head & _] (class head)))
(defmethod register-udf! :default
  ([udf-name f return-type] (register! @defaults/spark udf-name f return-type {}))
  ([udf-name f return-type opts] (register! @defaults/spark udf-name f return-type opts)))
(defmethod register-udf! SparkSession
  ([spark udf-name f return-type] (register! spark udf-name f return-type {}))
  ([spark udf-name f return-type opts] (register! spark udf-name f return-type opts)))
