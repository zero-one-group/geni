;; Docstring Sources:
;; - https://github.com/apache/spark/blob/v3.0.1/sql/catalyst/src/main/java/org/apache/spark/sql/types/DataTypes.java
(ns zero-one.geni.core.dataset-creation
  (:refer-clojure :exclude [range])
  (:require
   [zero-one.geni.defaults :as defaults]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.utils :refer [class-named]])
  (:import
   (clojure.lang Reflector)
   (org.apache.spark.sql.types ArrayType DataType DataTypes)
   (org.apache.spark.sql SparkSession)))

(def ^:private vector-udt
  "MLlib's vector type, when spark-mllib is on the classpath. It's classic
  Spark only, so a Spark Connect client doesn't have it."
  (some-> (class-named "org.apache.spark.ml.linalg.VectorUDT")
          (Reflector/invokeConstructor (object-array 0))))

(def ^:private variant-type
  "Spark 4's VARIANT type, for a VariantVal, or nil on Spark 3.5."
  (some-> (class-named "org.apache.spark.sql.types.VariantType$")
          (.getField "MODULE$")
          (.get nil)))

(def ^:private time-type
  "Spark 4.1's TIME type, at microseconds, for a LocalTime, or nil before 4.1."
  (some-> (class-named "org.apache.spark.sql.types.TimeType")
          (Reflector/invokeConstructor (object-array [(int 6)]))))

(def data-type->spark-type
  "A mapping from type keywords to Spark types."
  (cond-> {:bool      DataTypes/BooleanType
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
           nil        DataTypes/NullType}
    vector-udt (assoc :vector vector-udt)))

(defn- ->spark-type
  "The Spark type for a type keyword. Without spark-mllib, as with a Spark
  Connect client, :vector says what it needs."
  [data-type]
  (or (data-type->spark-type data-type)
      (when (= :vector data-type)
        (interop/mllib-class "org.apache.spark.ml.linalg.VectorUDT"))))

(defn struct-field
  "Creates a StructField by specifying the name `col-name`, data type `data-type`
  and whether values of this field can be null values `nullable`."
  [col-name data-type nullable]
  (let [spark-type (if (instance? DataType data-type)
                     data-type
                     (->spark-type data-type))]
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
                     (->spark-type val-type))]
    (DataTypes/createArrayType spark-type nullable)))

(defn map-type
  "Creates a MapType by specifying the data type of keys `key-type`, the data type
   of values `val-type`, and whether values contain any null value `nullable`."
  [key-type val-type]
  (DataTypes/createMapType
   (->spark-type key-type)
   (->spark-type val-type)))

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

(defn parse-ddl
  "Parses a DDL string into a Spark type: a schema such as
  `\"id BIGINT, name STRING\"` into a struct type, and a type such as
  `\"ARRAY<STRING>\"` into that type. It runs where Geni runs, with no Spark
  job, over Spark Connect too.

  ```clojure
  (g/parse-ddl \"id BIGINT, tags ARRAY<STRING>\")
  ```"
  ^DataType [^String ddl]
  (DataType/fromDDL ddl))

(defn- empty-schema? [schema]
  (if (coll? schema)
    (empty? schema)
    false))

(declare tmd-dataset? tmd->dataframe)

(defn create-dataframe
  "Creates a DataFrame from a tech.ml.dataset dataset, or from rows and a
  schema, on the default session or the one given.

  From a dataset, each column's datatype gives its Spark type: `:int32`
  INT, `:float64` DOUBLE, `:string` STRING, `:local-date` DATE,
  `:instant` TIMESTAMP, `:local-date-time` TIMESTAMP_NTZ, `:duration` a
  day-time interval, `:decimal` DECIMAL(38,18), and so on, packed or not.
  Columns of other objects, such as vectors and maps, get their types
  inferred from their values, as `records->dataset` does. A missing value
  is a null. `to-tmd` goes the other way.

  ```clojure
  (g/create-dataframe (tech.v3.dataset/->dataset {:a [1 2] :b [\"x\" nil]}))
  ```

  From rows, a java.util.List of Spark Rows, `schema` is a StructType, or
  plain Clojure data that `->schema` takes."
  ([dataset] (create-dataframe @defaults/spark dataset))
  ([spark-or-rows dataset-or-schema]
   (if (instance? SparkSession spark-or-rows)
     (if (tmd-dataset? dataset-or-schema)
       (tmd->dataframe spark-or-rows dataset-or-schema)
       (throw (ex-info (str "create-dataframe takes a tech.ml.dataset dataset after a session, "
                            "or rows and a schema.")
                       {})))
     (create-dataframe @defaults/spark spark-or-rows dataset-or-schema)))
  ([spark rows schema]
   (if (and (empty? rows) (empty-schema? schema))
     (.emptyDataFrame spark)
     (.createDataFrame spark rows (->schema schema)))))

(def java-type->spark-type
  "A mapping from Java types to Spark types, for inferring a schema from
  Clojure data. Keywords and UUIDs become strings, and a `java.util.Date`, such
  as `#inst`, a timestamp."
  (cond-> {java.lang.Boolean       DataTypes/BooleanType
           java.lang.Byte          DataTypes/ByteType
           java.lang.Double        DataTypes/DoubleType
           java.lang.Float         DataTypes/FloatType
           java.lang.Integer       DataTypes/IntegerType
           java.lang.Long          DataTypes/LongType
           java.lang.Short         DataTypes/ShortType
           java.lang.String        DataTypes/StringType
           ;; Spark's own defaults, as for a Java bean's fields.
           java.math.BigDecimal    (DataTypes/createDecimalType 38 18)
           java.math.BigInteger    (DataTypes/createDecimalType 38 0)
           clojure.lang.BigInt     (DataTypes/createDecimalType 38 0)
           java.time.LocalDate     DataTypes/DateType
           java.sql.Date           DataTypes/DateType
           java.time.Instant       DataTypes/TimestampType
           java.sql.Timestamp      DataTypes/TimestampType
           java.util.Date          DataTypes/TimestampType
           java.time.LocalDateTime DataTypes/TimestampNTZType
           java.time.Duration      (DataTypes/createDayTimeIntervalType)
           java.time.Period        (DataTypes/createYearMonthIntervalType)
           clojure.lang.Keyword    DataTypes/StringType
           java.util.UUID          DataTypes/StringType
           (Class/forName "[B")    DataTypes/BinaryType
           nil                     DataTypes/NullType}
    vector-udt   (assoc (class-named "org.apache.spark.ml.linalg.DenseVector") vector-udt
                        (class-named "org.apache.spark.ml.linalg.SparseVector") vector-udt)
    variant-type (assoc (class-named "org.apache.spark.unsafe.types.VariantVal") variant-type)
    time-type    (assoc java.time.LocalTime time-type)))

(def ^:private value-conversions
  "How a value of each of these classes becomes one that Spark takes for the
  type that java-type->spark-type gives it. Dates and times become java.sql
  ones, which a Spark Connect client takes whatever
  `spark.sql.datetime.java8API.enabled` says, as classic Spark does. The
  lookup is by exact class, so java.util.Date's conversion leaves a
  java.sql.Date or java.sql.Timestamp alone."
  {clojure.lang.BigInt  #(BigDecimal. (.toBigInteger ^clojure.lang.BigInt %))
   java.math.BigInteger #(BigDecimal. ^java.math.BigInteger %)
   clojure.lang.Keyword #(subs (str %) 1)
   java.util.UUID       str
   java.time.LocalDate  #(java.sql.Date/valueOf ^java.time.LocalDate %)
   java.time.Instant    #(java.sql.Timestamp/from ^java.time.Instant %)
   java.util.Date       #(java.sql.Timestamp. (.getTime ^java.util.Date %))})

(declare infer-schema infer-spark-type)

(defn- infer-spark-type [col-name value]
  (cond
    (map? value)  (infer-schema (map name (keys value)) (vals value))
    (coll? value) (ArrayType. (infer-spark-type col-name (first value)) true)
    :else         (let [cls (class value)]
                    (or (get java-type->spark-type cls)
                        (throw (ex-info (str "Can't infer a Spark type for the column \"" col-name
                                             "\" from a " (.getName ^Class cls) ". Convert its "
                                             "values first, to one of the types that the "
                                             "manual dataset creation guide lists.")
                                        {:column col-name :class cls}))))))

(defn- infer-struct-field [col-name value]
  (let [spark-type   (infer-spark-type col-name value)]
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
    :else (if-let [convert (value-conversions (class value))]
            (convert value)
            value)))

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

;; tech.ml.dataset

(defn- tmd-dataset?
  "Whether `value` is a tech.ml.dataset dataset. When tech.ml.dataset isn't
  loaded, nothing is."
  [value]
  (boolean (when-let [dataset? (resolve 'tech.v3.dataset.impl.dataset/dataset?)]
             (dataset? value))))

(def ^:private tmd-type->spark-type
  "The Spark type for a tech.ml.dataset column's datatype, when it has one.
  Columns of other datatypes get theirs inferred from their values."
  (cond-> {:boolean              DataTypes/BooleanType
           :int8                 DataTypes/ByteType
           :int16                DataTypes/ShortType
           :int32                DataTypes/IntegerType
           :int64                DataTypes/LongType
           :uint8                DataTypes/ShortType
           :uint16               DataTypes/IntegerType
           :uint32               DataTypes/LongType
           :uint64               (DataTypes/createDecimalType 20 0)
           :float32              DataTypes/FloatType
           :float64              DataTypes/DoubleType
           :string               DataTypes/StringType
           :text                 DataTypes/StringType
           :keyword              DataTypes/StringType
           :uuid                 DataTypes/StringType
           :local-date           DataTypes/DateType
           :packed-local-date    DataTypes/DateType
           :instant              DataTypes/TimestampType
           :packed-instant       DataTypes/TimestampType
           :packed-milli-instant DataTypes/TimestampType
           :zoned-date-time      DataTypes/TimestampType
           :local-date-time      DataTypes/TimestampNTZType
           :duration             (DataTypes/createDayTimeIntervalType)
           :packed-duration      (DataTypes/createDayTimeIntervalType)
           :decimal              (DataTypes/createDecimalType 38 18)}
    time-type (assoc :local-time time-type :packed-local-time time-type)))

(def ^:private tmd-coercions
  "How a value that a tech.ml.dataset column of each of these datatypes reads
  out, which dtype-next widens to a long or a double, becomes one of the
  class that its Spark type takes."
  {:int8    byte
   :int16   short
   :uint8   short
   :int32   int
   :uint16  int
   :uint32  long
   :int64   long
   :uint64  #(BigDecimal. (Long/toUnsignedString (long %)))
   :float32 float
   :float64 double
   :text    str})

(defn- tmd-value
  "A value of a tech.ml.dataset column of `datatype`, as Spark takes it."
  [datatype value]
  (cond
    (nil? value)                              nil
    (instance? java.time.ZonedDateTime value) (java.sql.Timestamp/from
                                               (.toInstant ^java.time.ZonedDateTime value))
    :else                                     (if-let [coerce (tmd-coercions datatype)]
                                                (coerce value)
                                                value)))

(defn- column-name
  "A tech.ml.dataset column name as a Spark one: a keyword without its colon."
  [col-name]
  (if (keyword? col-name) (subs (str col-name) 1) (str col-name)))

(defn- tmd->dataframe
  "A DataFrame of a tech.ml.dataset dataset, through rows on the driver."
  [spark dataset]
  (let [columns   ((requiring-resolve 'tech.v3.dataset/columns) dataset)
        names     (map #(column-name (:name (meta %))) columns)
        datatypes (map #(:datatype (meta %)) columns)
        values    (map (fn [column datatype] (mapv #(tmd-value datatype %) column))
                       columns
                       datatypes)
        samples   (map first-non-nil values)
        values    (map fill-missing-nested-keys values samples)
        fields    (map (fn [col-name datatype sample]
                         (if-let [spark-type (tmd-type->spark-type datatype)]
                           (DataTypes/createStructField col-name spark-type true)
                           (infer-struct-field col-name (first sample))))
                       names
                       datatypes
                       samples)
        rows      (if (seq columns) (transpose values) [])]
    (.createDataFrame spark
                      (interop/->java-list (map interop/->spark-row (transform-maps rows)))
                      (DataTypes/createStructType ^java.util.List (vec fields)))))

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

