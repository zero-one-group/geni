;; Docstring Sources:
;; - https://github.com/apache/spark/blob/v3.0.1/sql/catalyst/src/main/java/org/apache/spark/sql/types/DataTypes.java
(ns zero-one.geni.core.dataset-creation
  (:refer-clojure :exclude [range])
  (:require
   [zero-one.geni.defaults :as defaults]
   [zero-one.geni.docs :as docs]
   [zero-one.geni.interop :as interop])
  (:import
   (org.apache.spark.sql.types ArrayType DataType DataTypes)
   (org.apache.spark.ml.linalg VectorUDT
                               DenseVector
                               SparseVector)
   (org.apache.spark.sql SparkSession)))

(def data-type->spark-type
  "A mapping from type keywords to Spark types."
  {:bool      DataTypes/BooleanType
   :boolean   DataTypes/BooleanType
   :byte      DataTypes/ByteType
   :date      DataTypes/DateType
   :double    DataTypes/DoubleType
   :float     DataTypes/FloatType
   :int       DataTypes/IntegerType
   :integer   DataTypes/IntegerType
   :long      DataTypes/LongType
   :nil       DataTypes/NullType
   :short     DataTypes/ShortType
   :str       DataTypes/StringType
   :string    DataTypes/StringType
   :timestamp DataTypes/TimestampType
   :vector    (VectorUDT.)
   nil        DataTypes/NullType})

(defn struct-field
  "Creates a StructField by specifying the name `col-name`, data type `data-type`
  and whether values of this field can be null values `nullable`."
  [col-name data-type nullable]
  (let [spark-type (if (instance? DataType data-type)
                     data-type
                     (data-type->spark-type data-type))]
    (DataTypes/createStructField (name col-name) spark-type nullable)))

(defn struct-type
  "Creates a StructType with the given list of StructFields `fields`."
  [& fields]
  (DataTypes/createStructType fields))

(defn array-type
  "Creates an ArrayType by specifying the data type of elements `val-type` and
   whether the array contains null values `nullable`."
  [val-type nullable]
  (let [spark-type (if (instance? DataType val-type)
                     val-type
                     (data-type->spark-type val-type))]
    (DataTypes/createArrayType spark-type nullable)))

(defn map-type
  "Creates a MapType by specifying the data type of keys `key-type`, the data type
   of values `val-type`, and whether values contain any null value `nullable`."
  [key-type val-type]
  (DataTypes/createMapType
   (data-type->spark-type key-type)
   (data-type->spark-type val-type)))

(defn ->schema
  "Coerces plain Clojure data structures to a Spark schema.

  ```clojure
  (-> {:x [:short]
       :y [:string :int]
       :z {:a :float :b :double}}
      g/->schema
      g/->string)
  => StructType(
       StructField(x,ArrayType(ShortType,true),true),
       StructField(y,MapType(StringType,IntegerType,true),true),
       StructField(
         z,
         StructType(
           StructField(a,FloatType,true),
           StructField(b,DoubleType,true)
         ),
         true
       )
     )
  ```"
  [value]
  (cond
    (and (vector? value) (= 1 (count value)))
    (array-type (->schema (first value)) true)

    (and (vector? value) (= 2 (count value)))
    (map-type (->schema (first value)) (->schema (second value)))

    (map? value)
    (->> value
         (map (fn [[k v]] (struct-field k (->schema v) true)))
         (apply struct-type))

    :else
    value))

(defn- empty-schema? [schema]
  (if (coll? schema)
    (empty? schema)
    false))

(defn create-dataframe
  ([rows schema] (create-dataframe @defaults/spark rows schema))
  ([spark rows schema]
   (if (and (empty? rows) (empty-schema? schema))
     (.emptyDataFrame spark)
     (.createDataFrame spark rows (->schema schema)))))

(def java-type->spark-type
  "A mapping from Java types to Spark types."
  {java.lang.Boolean  DataTypes/BooleanType
   java.lang.Byte     DataTypes/ByteType
   java.lang.Double   DataTypes/DoubleType
   java.lang.Float    DataTypes/FloatType
   java.lang.Integer  DataTypes/IntegerType
   java.lang.Long     DataTypes/LongType
   java.lang.Short    DataTypes/ShortType
   java.lang.String   DataTypes/StringType
   java.sql.Timestamp DataTypes/TimestampType
   java.util.Date     DataTypes/DateType
   DenseVector        (VectorUDT.)
   SparseVector       (VectorUDT.)
   nil                DataTypes/NullType})

(declare infer-schema infer-spark-type)

(defn- infer-spark-type [value]
  (cond
    (map? value) (infer-schema (map name (keys value)) (vals value))
    (coll? value) (ArrayType. (infer-spark-type (first value)) true)
    :else (get java-type->spark-type (type value) DataTypes/BinaryType)))

(defn- infer-struct-field [col-name value]
  (let [spark-type   (infer-spark-type value)]
    (DataTypes/createStructField col-name spark-type true)))

(defn- infer-schema [col-names values]
  (DataTypes/createStructType
   (mapv infer-struct-field col-names values)))

(defn- update-val-in
  "Works similar to update-in but accepts value instead of function.
   If old and new values are collections, merges/concatenates them.
   If the associative structure is nil, initialises it to provided value."
  [m path val]
  (if-not m val
          (update-in m path (fn [old new]
                              (cond
                                (nil? old) new
                                (and (map? old) (map? new)) (merge old new)
                                (and (coll? new) (= (type old) (type new))) (into old new)
                                :else new)) val)))

(defn- first-non-nil
  "Looks through values and recursively finds the first non-nil value.
   For maps, it returns a first non-nil value for each nested key.
   For list of maps, it returns a list of one map with first non-nil value for each nested key.
     
     Examples:
     []                                          => []
     [nil nil]                                   => []
     [1 2 3]                                     => [1]
     [nil [1 2]]                                 => [[1]]
     [{:a 1} {:a 3 :b true}]                     => [{:a 1 :b true}]
     [{:a 1} {:b [{:a 4} {:c 3}]}]               => [{:a 1 :b [{:a 4 :c 3}]}]
     [{:a 1} {:b [[{:a 4} {:c 3}] [{:h true}]]}] => [{:a 1 :b [[{:a 4 :c 3 :h true}]]}]"
  ([v]
   (first-non-nil v nil []))
  ([v non-nil path]
   (cond (map? v) (reduce #(first-non-nil (get v %2) %1 (conj path %2)) (update-val-in non-nil path {}) (keys v))
         (coll? v) (reduce (fn [non-nil v]
                             (let [path (conj path 0)
                                   non-nil (first-non-nil v non-nil path)]
                               (if (coll? (get-in non-nil path)) non-nil (reduced non-nil))))
                           (update-val-in non-nil path []) (filter (complement nil?) v))
         (or (nil? v) (some? (get-in non-nil path))) non-nil
         :else (update-val-in non-nil path v))))

(defn- fill-missing-nested-keys
  "Recursively fills in any missing keys. Takes as input the records and a sample non-nil value.
   The sample non-nil value can be generated using first-non-nil function above.
   
     Examples:
     [] | []
      => []
     [nil nil] | []
      => [nil nil]
     [1 2 3] | [1]
      => [1 2 3]
     [nil [1 2]] | [[1]]
      => [nil [1 2]]
     [{:a 1} {:a 3 :b true}] | [{:a 1 :b true}]
      => [{:a 1 :b nil} {:a 3 :b true}]
     [{:a 1} {:b [{:a 4} {:c 3}]}] | [{:a 1 :b [{:a 4 :c 3}]}])
      => [{:a 1 :b nil} {:a nil :b [{:a 4 :c nil} {:a nil :c 3}]}]
     [{:a 1} {:b [[{:a 4} {:c 3}] [{:h true}]]}] | [{:a 1 :b [[{:a 4 :c 3 :h true}]]}]
      => [{:a 1 :b nil} {:a nil :b [[{:a 4 :c nil :h nil} {:a nil :c 3 :h nil}] [{:a nil :c nil :h true}]]}]"
  ([v non-nil]
   (fill-missing-nested-keys v non-nil []))
  ([v non-nil path]
   (cond
     (map? v) (reduce #(assoc %1 %2 (fill-missing-nested-keys (get v %2) non-nil (conj path %2)))
                      {} (keys (get-in non-nil path)))
     (and (coll? v)
          (coll? (get-in non-nil (conj path 0)))) (map #(fill-missing-nested-keys % non-nil (conj path 0)) v)
     :else v)))

(defn- transpose [xs]
  (apply map list xs))

(defn- transform-maps
  [value]
  (cond
    (map? value) (interop/->spark-row (transform-maps (vals value)))
    (coll? value) (map transform-maps value)
    :else value))

(defn table->dataset
  "Construct a Dataset from a collection of collections.

  ```clojure
  (g/show (g/table->dataset [[1 2] [3 4]] [:a :b]))
  ; +---+---+
  ; |a  |b  |
  ; +---+---+
  ; |1  |2  |
  ; |3  |4  |
  ; +---+---+
  ```"
  ([table col-names] (table->dataset @defaults/spark table col-names))
  ([spark table col-names]
   (if (empty? table)
     (.emptyDataFrame spark)
     (let [col-names  (map name col-names)
           transposed (transpose table)
           values     (map first-non-nil transposed)
           table      (transpose (map (partial apply fill-missing-nested-keys) (map vector transposed values)))
           rows       (interop/->java-list (map interop/->spark-row (transform-maps table)))
           schema     (infer-schema col-names (map first values))]
       (.createDataFrame spark rows schema)))))

(defn map->dataset
  "Construct a Dataset from an associative map.

  ```clojure
  (g/show (g/map->dataset {:a [1 2], :b [3 4]}))
  ; +---+---+
  ; |a  |b  |
  ; +---+---+
  ; |1  |3  |
  ; |2  |4  |
  ; +---+---+
  ```"
  ([map-of-values] (map->dataset @defaults/spark map-of-values))
  ([spark map-of-values]
   (if (empty? map-of-values)
     (.emptyDataFrame spark)
     (let [table     (transpose (vals map-of-values))
           col-names (keys map-of-values)]
       (table->dataset spark table col-names)))))

(defn- conj-record [map-of-values record]
  (let [col-names (keys map-of-values)]
    (reduce
     (fn [acc-map col-name]
       (update acc-map col-name #(conj % (get record col-name))))
     map-of-values
     col-names)))

(defn records->dataset
  "Construct a Dataset from a collection of maps.

  ```clojure
  (g/show (g/records->dataset [{:a 1 :b 2} {:a 3 :b 4}]))
  ; +---+---+
  ; |a  |b  |
  ; +---+---+
  ; |1  |2  |
  ; |3  |4  |
  ; +---+---+
  ```"
  ([records] (records->dataset @defaults/spark records))
  ([spark records]
   (let [col-names     (-> (map keys records) flatten distinct)
         map-of-values (reduce
                        conj-record
                        (zipmap col-names (repeat []))
                        records)]
     (map->dataset spark map-of-values))))

(defmulti range
  "Creates a `Dataset` with a single `LongType` column named `id`.

  The `Dataset` contains elements in a range from `start` (default 0) to `end` (exclusive)
  with the given `step` (default 1).

  If `num-partitions` is specified, the dataset will be distributed into the specified number
  of partitions. Otherwise, spark uses internal logic to determine the number of partitions."
  (fn [& args] (mapv class args)))
(defmethod range [Long]
  [^Long end]
  (range @defaults/spark end))
(defmethod range [Long Long]
  [^Long start ^Long end]
  (range @defaults/spark start end))
(defmethod range [Long Long Long]
  [^Long start ^Long end ^Long step]
  (range @defaults/spark start end step))
(defmethod range [Long Long Long Long]
  [^Long start ^Long end ^Long step ^Integer num-partitions]
  (range @defaults/spark start end step num-partitions))
(defmethod range [SparkSession Long]
  [^SparkSession spark ^Long end]
  (.range spark end))
(defmethod range [SparkSession Long Long]
  [^SparkSession spark ^Long start ^Long end]
  (.range spark start end))
(defmethod range [SparkSession Long Long Long]
  [^SparkSession spark ^Long start ^Long end ^Long step]
  (.range spark start end step))
(defmethod range [SparkSession Long Long Long Long]
  [^SparkSession spark ^Long start ^Long end ^Long step ^Integer num-partitions]
  (.range spark start end step num-partitions))

;; Docs
(docs/add-doc!
 (var create-dataframe)
 (-> docs/spark-docs :methods :spark :session :create-data-frame))
