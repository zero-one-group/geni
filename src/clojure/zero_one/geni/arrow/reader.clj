(ns zero-one.geni.arrow.reader
  "A DataFrame's result as Arrow IPC streams, and those streams decoded into
  the columns that tech.ml.dataset and dtype-next take. Loaded when it's first
  needed, since it needs Apache Arrow, which classic Spark brings and a Spark
  Connect client only has shaded. zero-one.geni.core.results has the public
  functions."
  (:require
   [zero-one.geni.utils :refer [class-named]])
  (:import
   (clojure.lang Reflector)
   (java.io ByteArrayInputStream ByteArrayOutputStream)
   (java.nio.channels Channels)
   (java.nio.charset StandardCharsets)
   (java.time Instant LocalDate LocalTime Period)
   (java.util Iterator List Map)
   (org.apache.arrow.memory BufferAllocator RootAllocator)
   (org.apache.arrow.vector BigIntVector BitVector DateDayVector DecimalVector DurationVector
                            FieldVector Float4Vector Float8Vector IntVector IntervalYearVector
                            LargeVarBinaryVector LargeVarCharVector NullVector SmallIntVector
                            TimeNanoVector TimeStampMicroTZVector TimeStampMicroVector TinyIntVector
                            VarBinaryVector VarCharVector VectorSchemaRoot VectorUnloader)
   (org.apache.arrow.vector.complex ListVector StructVector)
   (org.apache.arrow.vector.ipc ArrowStreamReader ArrowStreamWriter WriteChannel)
   (org.apache.arrow.vector.ipc.message IpcOption MessageSerializer)
   (org.apache.arrow.vector.types.pojo Schema)
   (org.apache.spark.sql.types ArrayType DataType DateType MapType StringType StructField
                               StructType TimestampType UserDefinedType YearMonthIntervalType)))

(set! *warn-on-reflection* true)

(def ^:private allocator
  "One root allocator for the decoding. Each stream gets a child of it, closed
  once its values are copied out."
  (delay (RootAllocator.)))

;; Classic Spark's batches

(defn- conf ^String [spark ^String k ^String default]
  (.get (.conf ^org.apache.spark.sql.SparkSession spark) k default))

(defn- arrow-schema
  "The Arrow schema that Spark's own batches for `df` follow, built as
  `Dataset.toArrowBatchRdd` builds it: Spark 3.5 never uses large var types,
  and Spark 4 does when spark.sql.execution.arrow.useLargeVarTypes says so.
  ArrowUtils is private to Spark's sql package, so it's looked up when it's
  called, which also keeps this namespace loading with a Connect client."
  ^Schema [df]
  (let [spark (.sparkSession ^org.apache.spark.sql.Dataset df)
        large (and (not (.startsWith (.version ^org.apache.spark.sql.SparkSession spark) "3."))
                   (= "true" (conf spark "spark.sql.execution.arrow.useLargeVarTypes" "false")))]
    (Reflector/invokeStaticMethod
     "org.apache.spark.sql.util.ArrowUtils" "toArrowSchema"
     (object-array [(.schema ^org.apache.spark.sql.Dataset df)
                    (conf spark "spark.sql.session.timeZone" "UTC")
                    (= "legacy" (conf spark "spark.sql.execution.pandas.structHandlingMode" "legacy"))
                    large]))))

(defn- ipc-stream
  "A complete Arrow IPC stream: the schema, the record batch messages that
  Spark serialised, and the end marker."
  ^bytes [^Schema schema batches]
  (let [out     (ByteArrayOutputStream.)
        channel (WriteChannel. (Channels/newChannel out))]
    (MessageSerializer/serialize channel schema)
    (doseq [^bytes batch batches]
      (.write channel batch))
    (ArrowStreamWriter/writeEndOfStream channel IpcOption/DEFAULT)
    (.toByteArray out)))

(defn- empty-batch
  "A serialised record batch with no rows, for a result with none, which
  Spark sends no batch for."
  ^bytes [^Schema schema]
  (with-open [alloc (.newChildAllocator ^BufferAllocator @allocator "geni-empty" 0 Long/MAX_VALUE)
              root  (VectorSchemaRoot/create schema alloc)]
    (.setRowCount root 0)
    (with-open [batch (.getRecordBatch (VectorUnloader. root))]
      (let [out (ByteArrayOutputStream.)]
        (MessageSerializer/serialize (WriteChannel. (Channels/newChannel out)) batch)
        (.toByteArray out)))))

(defn- call [target method & args]
  (Reflector/invokeInstanceMethod target method (object-array args)))

(defn- arrow-batch-rdd
  "Spark's own Arrow batches for `df`, as `toPandas` gets them in PySpark:
  `Dataset.toArrowBatchRdd`, an RDD of serialised record batches, at most
  spark.sql.execution.arrow.maxRecordsPerBatch rows each. It's private to
  Spark's sql package, and present from Spark 3.5 to 4.2. Returns the RDD
  as a JavaRDD."
  [df]
  (call (call df "toArrowBatchRdd") "toJavaRDD"))

(defn classic-streams
  "The result of `df` on classic Spark as complete Arrow IPC streams, one per
  batch. With `:all`, one job collects every batch, and an empty result gives
  one stream with no rows. With `:lazy`, the batches come one partition at a
  time, a job each, as the iterator is read. Returns a java.util.Iterator."
  ^Iterator [df mode]
  (let [schema (arrow-schema df)
        rdd    (arrow-batch-rdd df)]
    (case mode
      :all  (let [batches (vec (call rdd "collect"))
                  ^List streams (if (seq batches)
                                  (mapv #(ipc-stream schema [%]) batches)
                                  [(ipc-stream schema [(empty-batch schema)])])]
              (.iterator streams))
      :lazy (let [^Iterator batches (call rdd "toLocalIterator")]
              (reify Iterator
                (hasNext [_] (.hasNext batches))
                (next [_] (ipc-stream schema [(.next batches)])))))))

;; Decoding

(def ^:private variant-type (delay (class-named "org.apache.spark.sql.types.VariantType")))
(def ^:private time-type (delay (class-named "org.apache.spark.sql.types.TimeType")))

(defn- instance-of? [class-delay value]
  (boolean (some-> ^Class @class-delay (.isInstance value))))

(defn- vector-udt? [dt]
  (= "org.apache.spark.ml.linalg.VectorUDT" (.getName (class dt))))

(defn- micros->instant ^Instant [^long micros]
  (Instant/ofEpochSecond (Math/floorDiv micros 1000000)
                         (* 1000 (Math/floorMod micros 1000000))))

(defn- variant-val
  "Spark's own VariantVal, as `collect` gives for a VARIANT value, whose
  string is the value as JSON."
  [value metadata]
  (Reflector/invokeConstructor
   (class-named "org.apache.spark.unsafe.types.VariantVal")
   (object-array [value metadata])))

(defn- mllib-vector
  "MLlib's vector as `collect` gives it: a dense one as a vector of doubles,
  and a sparse one as a map of its size, indices and values."
  [^Map m]
  (if (= 1 (long (.get m "type")))
    (vec (.get m "values"))
    {:size    (.get m "size")
     :indices (vec (.get m "indices"))
     :values  (vec (.get m "values"))}))

(defn- value-converter
  "A function from the Java value that Arrow's `getObject` gives for a value
  of Spark type `dt`, inside an array, struct or map, to the Clojure value
  that the column holds."
  [^DataType dt]
  (cond
    (instance? StringType dt)            #(some-> % str)
    (instance? DateType dt)              #(some-> % long LocalDate/ofEpochDay)
    (instance? TimestampType dt)         #(some-> % long micros->instant)
    (instance? YearMonthIntervalType dt) (fn [^Period p] (when p (.normalized p)))
    (instance-of? time-type dt)          #(some-> % long LocalTime/ofNanoOfDay)
    (instance-of? variant-type dt)       (fn [^Map m] (when m (variant-val (.get m "value")
                                                                           (.get m "metadata"))))
    (vector-udt? dt)                     (fn [^Map m] (when m (mllib-vector m)))
    (instance? UserDefinedType dt)       (value-converter (.sqlType ^UserDefinedType dt))
    (instance? ArrayType dt)             (let [f (value-converter (.elementType ^ArrayType dt))]
                                           (fn [^List xs] (when xs (mapv f xs))))
    (instance? MapType dt)               (let [kf (value-converter (.keyType ^MapType dt))
                                               vf (value-converter (.valueType ^MapType dt))]
                                           (fn [^List entries]
                                             (when entries
                                               (into {}
                                                     (map (fn [^Map e]
                                                            [(kf (.get e "key")) (vf (.get e "value"))]))
                                                     entries))))
    (instance? StructType dt)            (let [fields (mapv (fn [^StructField field]
                                                              [(.name field)
                                                               (keyword (.name field))
                                                               (value-converter (.dataType field))])
                                                            (.fields ^StructType dt))]
                                           (fn [^Map m]
                                             (when m
                                               (persistent!
                                                (reduce (fn [acc [n k f]] (assoc! acc k (f (.get m n))))
                                                        (transient {})
                                                        fields)))))
    :else                                identity))

(defn- missing
  "The indices of the vector's nulls, or nil when it has none."
  ^ints [^FieldVector v]
  (let [n-nulls (.getNullCount v)]
    (when (pos? n-nulls)
      (let [out (int-array n-nulls)]
        (loop [i 0 j 0]
          (when (< j n-nulls)
            (if (.isNull v i)
              (do (aset out j i) (recur (inc i) (inc j)))
              (recur (inc i) j))))
        out))))

(defmacro ^:private fill
  "A new array, made with `ctor`, of the `n` values of the vector `v`: the
  body of `(fn [i] expr)` with `i` bound to each index, inlined, or `on-null`
  where `v` has a null."
  [ctor v n [_fn [i] expr] on-null]
  (let [a (gensym "a")]
    `(let [n# (int ~n)
           ~a (~ctor n#)]
       (dotimes [~i n#]
         (if (.isNull ~v ~i)
           ~(when on-null `(aset ~a ~i ~on-null))
           (aset ~a ~i ~expr)))
       ~a)))

(defn- objects
  "The vector's values as an object array, through `f`, nil for a null."
  ^objects [^FieldVector v n f]
  (let [a (object-array n)]
    (dotimes [i n]
      (when-not (.isNull v i)
        (aset a i (f i))))
    a))

(defn- column-data
  "A column's values, as an array, and the datatype that tech.ml.dataset
  gets for it."
  [^FieldVector v ^DataType dt]
  (let [n (.getValueCount v)]
    (condp instance? v
      BitVector              [:boolean (let [^BitVector v v] (fill boolean-array v n (fn [i] (== 1 (.get v i))) nil))]
      TinyIntVector          [:int8 (let [^TinyIntVector v v] (fill byte-array v n (fn [i] (.get v i)) nil))]
      SmallIntVector         [:int16 (let [^SmallIntVector v v] (fill short-array v n (fn [i] (.get v i)) nil))]
      IntVector              [:int32 (let [^IntVector v v] (fill int-array v n (fn [i] (.get v i)) nil))]
      BigIntVector           [:int64 (let [^BigIntVector v v] (fill long-array v n (fn [i] (.get v i)) nil))]
      Float4Vector           [:float32 (let [^Float4Vector v v] (fill float-array v n (fn [i] (.get v i)) Float/NaN))]
      Float8Vector           [:float64 (let [^Float8Vector v v] (fill double-array v n (fn [i] (.get v i)) Double/NaN))]
      DateDayVector          [:packed-local-date (let [^DateDayVector v v] (fill int-array v n (fn [i] (.get v i)) nil))]
      TimeStampMicroTZVector [:packed-instant (let [^TimeStampMicroTZVector v v] (fill long-array v n (fn [i] (.get v i)) nil))]
      ;; tech.ml.dataset packs a LocalTime as microseconds and a Duration as
      ;; nanoseconds, where Spark sends nanoseconds and microseconds.
      TimeNanoVector         [:packed-local-time
                              (let [^TimeNanoVector v v] (fill long-array v n (fn [i] (quot (.get v i) 1000)) nil))]
      DurationVector         [:packed-duration
                              (let [^DurationVector v v buf (.getDataBuffer v)]
                                (fill long-array v n (fn [i] (* 1000 (DurationVector/get buf i))) nil))]
      TimeStampMicroVector   [:local-date-time
                              (let [^TimeStampMicroVector v v] (objects v n #(.getObject v (int %))))]
      VarCharVector          [:string (let [^VarCharVector v v]
                                        (objects v n #(String. (.get v (int %)) StandardCharsets/UTF_8)))]
      LargeVarCharVector     [:string (let [^LargeVarCharVector v v]
                                        (objects v n #(String. (.get v (int %)) StandardCharsets/UTF_8)))]
      DecimalVector          [:decimal (let [^DecimalVector v v] (objects v n #(.getObject v (int %))))]
      VarBinaryVector        [:object (let [^VarBinaryVector v v] (objects v n #(.get v (int %))))]
      LargeVarBinaryVector   [:object (let [^LargeVarBinaryVector v v] (objects v n #(.get v (int %))))]
      IntervalYearVector     [:object (let [^IntervalYearVector v v]
                                        (objects v n #(.normalized ^Period (.getObject v (int %)))))]
      NullVector             [:object (object-array n)]
      (let [f (value-converter dt)]
        [(cond (instance? ArrayType dt)                    :persistent-vector
               (or (instance? MapType dt)
                   (instance? StructType dt))               :persistent-map
               :else                                        :object)
         (objects v n #(f (.getObject v (int %))))]))))

(defn- batch-columns
  "The batch loaded in `root`, as its row count and a map per column of its
  name, values, datatype and missing indices."
  [^VectorSchemaRoot root ^StructType schema]
  {:row-count (.getRowCount root)
   :columns   (mapv (fn [^FieldVector v ^StructField field]
                      (let [[datatype data] (column-data v (.dataType field))]
                        {:name     (.name field)
                         :datatype datatype
                         :data     data
                         :missing  (missing v)}))
                    (.getFieldVectors root)
                    (.fields schema))})

(defn- read-batches
  "Calls `f` on each batch's root in an Arrow IPC stream, or once on the
  empty root when the stream has no batch, and returns the results."
  [^bytes stream f]
  (with-open [alloc  (.newChildAllocator ^BufferAllocator @allocator "geni" 0 Long/MAX_VALUE)
              reader (ArrowStreamReader. (ByteArrayInputStream. stream) alloc)]
    (let [root (.getVectorSchemaRoot reader)]
      (loop [out []]
        (if (.loadNextBatch reader)
          (recur (conj out (f root)))
          (if (seq out) out [(f root)]))))))

(defn decode
  "The batches in an Arrow IPC stream from Spark, each as a map of its row
  count and its columns, which `schema`, the result's Spark schema, names
  and types."
  [^bytes stream ^StructType schema]
  (read-batches stream #(batch-columns % schema)))

;; Tensors

(def ^:private ^:dynamic *caller*
  "The public function that's decoding tensors, for its errors."
  "to-tensors")

(defn refuse!
  "Throws for a column that to-tensors can't take, saying why: `reason`
  follows the column's name, as in \", whose type is string\"."
  [col-name reason]
  (throw (ex-info (str *caller* " can't take the column \"" col-name "\"" reason ". It takes "
                       "numeric columns with no nulls, and arrays or dense MLlib vectors of "
                       "numbers, all of one length.")
                  {:column col-name})))

(defn- of-type [^DataType dt]
  (str ", whose type is " (.simpleString dt)))

(defmacro ^:private copy-range
  "A new array of the values of `v`, a vector of class `cls`, from `start`
  to `end`."
  [ctor cls v start end]
  (let [tv (vary-meta (gensym "v") assoc :tag cls)]
    `(let [~tv ~v
           start# (int ~start)
           n#     (- (int ~end) start#)
           a#     (~ctor n#)]
       (dotimes [i# n#]
         (aset a# i# (.get ~tv (int (+ start# i#)))))
       a#)))

(defn- numeric
  "The values of a numeric vector with no nulls, from `start` to `end`, as an
  array, or nil when it isn't numeric."
  [^FieldVector v col-name start end]
  (when (pos? (.getNullCount v))
    (refuse! col-name ", which has nulls"))
  (condp instance? v
    TinyIntVector  (copy-range byte-array TinyIntVector v start end)
    SmallIntVector (copy-range short-array SmallIntVector v start end)
    IntVector      (copy-range int-array IntVector v start end)
    BigIntVector   (copy-range long-array BigIntVector v start end)
    Float4Vector   (copy-range float-array Float4Vector v start end)
    Float8Vector   (copy-range double-array Float8Vector v start end)
    nil))

(defn- rows-of-numbers
  "The values of a list vector whose rows all hold `width` numbers, as one
  array, and the width. `dt` is the column's Spark type, for an error."
  [^ListVector v col-name dt]
  (let [n (.getValueCount v)]
    (when (pos? (.getNullCount v))
      (refuse! col-name ", which has nulls"))
    (let [start  (if (zero? n) 0 (.getElementStartIndex v 0))
          width  (if (zero? n) 0 (- (.getElementEndIndex v 0) start))]
      (dotimes [i n]
        (when-not (== width (- (.getElementEndIndex v i) (.getElementStartIndex v i)))
          (refuse! col-name ", whose arrays aren't all one length")))
      (let [^FieldVector values (.getDataVector v)]
        [(or (numeric values col-name start (+ start (* n width)))
             (refuse! col-name (of-type dt)))
         width]))))

(defn tensor-data
  "A column of the batch in `root`, by its index, as an array and a shape:
  [rows] for a numeric column, and [rows width] for arrays or dense vectors."
  [^VectorSchemaRoot root ^StructType schema ^long index]
  (let [^FieldVector v      (.getVector root (int index))
        ^StructField field  (aget (.fields schema) index)
        col-name            (.name field)
        n                   (.getValueCount v)
        dt                  (.dataType field)]
    (cond
      (vector-udt? dt)
      (let [^StructVector sv v
            ^TinyIntVector kind (.getChild sv "type")]
        (when (pos? (.getNullCount sv))
          (refuse! col-name ", which has nulls"))
        (dotimes [i n]
          (when-not (== 1 (.get kind i))
            (refuse! col-name ", which has sparse vectors")))
        (let [[data width] (rows-of-numbers (.getChild sv "values") col-name dt)]
          {:data data :shape [n width]}))

      (instance? ListVector v)
      (let [[data width] (rows-of-numbers v col-name dt)]
        {:data data :shape [n width]})

      :else
      {:data  (or (numeric v col-name 0 n)
                  (refuse! col-name (of-type dt)))
       :shape [n]})))

(defn decode-tensors
  "The columns at `indices` of each batch in an Arrow IPC stream, as
  tensor-data gives them, for the function named `caller`."
  [^bytes stream ^StructType schema indices caller]
  (binding [*caller* caller]
    (read-batches stream (fn [^VectorSchemaRoot root]
                           {:row-count (.getRowCount root)
                            :columns   (mapv #(tensor-data root schema %) indices)}))))
