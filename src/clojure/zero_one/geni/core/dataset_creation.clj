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
   (java.math BigDecimal RoundingMode)
   (java.time Duration Period)
   (org.apache.spark.sql.types ArrayType BooleanType ByteType DataType DataTypes DateType
                               DayTimeIntervalType DecimalType DoubleType FloatType IntegerType
                               LongType MapType ShortType StringType StructField
                               StructType TimestampNTZType TimestampType UserDefinedType
                               YearMonthIntervalType)
   (org.apache.spark.sql SparkSession)
   (scala.collection JavaConverters)))

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

  From a dataset, each column gets its Spark type from, in turn:
  - the `:schema` option, a map from column names to Spark types, each a
    DataType, a DDL string such as \"DECIMAL(12, 2)\", or what `->schema`
    takes;
  - the Spark type that `to-tmd` keeps in the column's metadata, under
    `:zero-one.geni/spark-type`, when the column still has the datatype
    that `to-tmd` gave it, so that a round trip keeps the types;
  - the column's datatype: `:int32` INT, `:float64` DOUBLE, `:string`
    STRING, `:local-date` DATE, `:instant` TIMESTAMP, `:local-date-time`
    TIMESTAMP_NTZ, `:duration` a day-time interval, and so on, packed or
    not. `:decimal` is a DECIMAL of 38 digits, 18 of them after the point,
    as Spark has for a BigDecimal, unless the values need more digits
    before the point or after it;
  - the values, as `records->dataset` infers them, for columns of other
    objects, such as vectors and maps.

  A missing value is a null. A float or a double goes into a DECIMAL as its
  shortest decimal, as Spark's `Decimal` reads a double. A value that its
  column's type can't hold exactly, such as a number with more digits after
  the point than its DECIMAL has, throws, naming the column, rather than
  being rounded or becoming a null. A dataset has no rows without a column, so neither does
  the DataFrame. `to-tmd` goes the other way.

  ```clojure
  (g/create-dataframe (tech.v3.dataset/->dataset {:a [1 2] :b [\"x\" nil]}))
  (g/create-dataframe dataset {:schema {:price \"DECIMAL(12, 2)\"}})
  ```

  From rows, a java.util.List of Spark Rows, `schema` is a StructType, or
  plain Clojure data that `->schema` takes."
  ([dataset] (create-dataframe @defaults/spark dataset))
  ([spark-or-rows dataset-or-schema]
   (cond
     (instance? SparkSession spark-or-rows)
     (if (tmd-dataset? dataset-or-schema)
       (tmd->dataframe spark-or-rows dataset-or-schema {})
       (throw (ex-info (str "create-dataframe takes a tech.ml.dataset dataset after a session, "
                            "or rows and a schema.")
                       {})))

     (tmd-dataset? spark-or-rows)
     (tmd->dataframe @defaults/spark spark-or-rows dataset-or-schema)

     :else
     (create-dataframe @defaults/spark spark-or-rows dataset-or-schema)))
  ([spark rows-or-dataset schema-or-options]
   (cond
     (tmd-dataset? rows-or-dataset)
     (tmd->dataframe spark rows-or-dataset schema-or-options)

     (and (empty? rows-or-dataset) (empty-schema? schema-or-options))
     (.emptyDataFrame spark)

     :else
     (.createDataFrame spark rows-or-dataset (->schema schema-or-options)))))

(def java-type->spark-type
  "A mapping from Java types to Spark types, for inferring a schema from
  Clojure data. Keywords and UUIDs become strings, and a `java.util.Date`, such
  as `#inst`, a timestamp. A DECIMAL here is where inference starts: the
  column's values then give its digits after the point, as `fit-decimals`
  says."
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

;; Decimals

(def ^:private max-precision
  "The most digits that Spark's DECIMAL holds."
  38)

(defn- ->big-decimal
  "A BigDecimal or a whole number as a BigDecimal, or nil for anything else."
  ^BigDecimal [value]
  (cond
    (instance? BigDecimal value)           value
    (instance? clojure.lang.BigInt value)  (BigDecimal. (.toBigInteger ^clojure.lang.BigInt value))
    (instance? BigInteger value)           (BigDecimal. ^BigInteger value)
    (integer? value)                       (BigDecimal/valueOf (long value))))

(defn- digits
  "A BigDecimal's digits before the point and after it, as a DECIMAL needs
  room for them: none before the point for a number under one, none after
  it for a negative scale, such as 1E+5's, and none for trailing zeros, such
  as 1.50's second."
  [^BigDecimal d]
  (let [d (.stripTrailingZeros d)
        d (if (neg? (.scale d)) (.setScale d 0) d)]
    [(max 0 (- (.precision d) (.scale d))) (.scale d)]))

(defn- field-name
  "The name of a struct field that the map key `k` gives, as `infer-schema`
  names it."
  [k]
  (if (or (keyword? k) (symbol? k) (string? k)) (name k) (str k)))

(defn- by-field-name
  "A map's values by the names of the struct fields that its keys give."
  [m]
  (persistent! (reduce-kv (fn [acc k v] (assoc! acc (field-name k) v)) (transient {}) m)))

(defn- decimal-type-for
  "The DECIMAL of 38 digits, Spark's widest, that holds every number in
  `values`, the column `col-name`'s, with `preferred-scale` digits after
  the point, as Spark gives a BigDecimal 18 and a whole number none, when
  the values leave room for that, and otherwise with as many as they need.
  Throws when no DECIMAL holds them."
  ^DecimalType [col-name preferred-scale values]
  (let [[whole fraction] (reduce (fn [[w f :as acc] value]
                                   (if-let [d (->big-decimal value)]
                                     (let [[dw df] (digits d)] [(max w dw) (max f df)])
                                     acc))
                                 [0 0]
                                 values)
        scale            (max fraction (min preferred-scale (- max-precision whole)))]
    (when (< max-precision (+ whole scale))
      (throw (ex-info (str "The column \"" col-name "\" has numbers with up to " whole " digits "
                           "before the point and " fraction " after it, which no DECIMAL holds, "
                           "since Spark's DECIMAL has at most " max-precision " digits. Round "
                           "them, or convert them to strings or doubles, first.")
                      {:column col-name :digits [whole fraction]})))
    (DataTypes/createDecimalType max-precision scale)))

(defn- has-decimal? [^DataType dt]
  (cond
    (instance? DecimalType dt) true
    (instance? ArrayType dt)   (has-decimal? (.elementType ^ArrayType dt))
    (instance? MapType dt)     (or (has-decimal? (.keyType ^MapType dt))
                                   (has-decimal? (.valueType ^MapType dt)))
    (instance? StructType dt)  (boolean (some #(has-decimal? (.dataType ^StructField %))
                                              (.fields ^StructType dt)))
    :else                      false))

(defn- fit-decimals
  "`dt`, a type that the first values of the column `col-name` gave, with
  each DECIMAL in it, at the top or inside arrays and structs, made to hold
  all the column's `values` there, as `decimal-type-for` makes it, and with
  the digits after the point that the inferred DECIMAL has, 18 or none, when
  they fit."
  ^DataType [col-name ^DataType dt values]
  (cond
    (not (has-decimal? dt))
    dt

    (instance? DecimalType dt)
    (decimal-type-for col-name (.scale ^DecimalType dt) values)

    (instance? ArrayType dt)
    (ArrayType. (fit-decimals col-name (.elementType ^ArrayType dt) (mapcat seq (filter coll? values)))
                (.containsNull ^ArrayType dt))

    (instance? MapType dt)
    (let [maps (filter map? values)]
      (DataTypes/createMapType (fit-decimals col-name (.keyType ^MapType dt) (mapcat keys maps))
                               (fit-decimals col-name (.valueType ^MapType dt) (mapcat vals maps))
                               (.valueContainsNull ^MapType dt)))

    (instance? StructType dt)
    (let [structs (map by-field-name (filter map? values))]
      (DataTypes/createStructType
       ^java.util.List
       (mapv (fn [^StructField field]
               (DataTypes/createStructField (.name field)
                                            (fit-decimals col-name (.dataType field)
                                                          (map #(get % (.name field)) structs))
                                            (.nullable field)))
             (.fields ^StructType dt))))

    :else
    dt))

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
           schema     (DataTypes/createStructType
                       ^java.util.List
                       (mapv (fn [col-name sample column]
                               (DataTypes/createStructField
                                col-name
                                (fit-decimals col-name (infer-spark-type col-name sample) column)
                                true))
                             col-names
                             (map first values)
                             transposed))]
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
   :float32 unchecked-float
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

;; The types that to-tmd keeps

(defn- recorded-datatypes
  "The datatypes that `to-tmd` gives a column of Spark type `dt`, packed and
  unpacked, as zero-one.geni.arrow.reader decodes it, or none for an MLlib
  vector, whose Spark type doesn't survive as DDL."
  [^DataType dt]
  (cond
    (instance? UserDefinedType dt)       #{}
    (instance? BooleanType dt)           #{:boolean}
    (instance? ByteType dt)              #{:int8}
    (instance? ShortType dt)             #{:int16}
    (instance? IntegerType dt)           #{:int32}
    (instance? LongType dt)              #{:int64}
    (instance? FloatType dt)             #{:float32}
    (instance? DoubleType dt)            #{:float64}
    (instance? StringType dt)            #{:string :text}
    (instance? DecimalType dt)           #{:decimal}
    (instance? DateType dt)              #{:packed-local-date :local-date}
    (instance? TimestampType dt)         #{:packed-instant :instant}
    (instance? TimestampNTZType dt)      #{:local-date-time}
    (instance? DayTimeIntervalType dt)   #{:duration :packed-duration}
    (instance? ArrayType dt)             #{:persistent-vector}
    (or (instance? MapType dt)
        (instance? StructType dt))       #{:persistent-map}
    (and time-type
         (instance? (class time-type) dt)) #{:packed-local-time :local-time}
    :else                                #{:object}))

(defn- ->nullable
  "`dt` with every value inside it nullable, as a DataFrame's columns are
  here: a struct's fields, an array's elements and a map's values."
  ^DataType [^DataType dt]
  (cond
    (instance? ArrayType dt)  (ArrayType. (->nullable (.elementType ^ArrayType dt)) true)
    (instance? MapType dt)    (DataTypes/createMapType (->nullable (.keyType ^MapType dt))
                                                       (->nullable (.valueType ^MapType dt))
                                                       true)
    (instance? StructType dt) (DataTypes/createStructType
                               ^java.util.List
                               (mapv (fn [^StructField field]
                                       (DataTypes/createStructField (.name field)
                                                                    (->nullable (.dataType field))
                                                                    true))
                                     (.fields ^StructType dt)))
    :else                     dt))

(defn- recorded-type
  "The Spark type that `to-tmd` kept in a column's metadata, when the column
  still has the datatype that `to-tmd` gave it, or nil. A type that this
  Spark doesn't have, such as Spark 4's VARIANT on Spark 3.5, is nil too."
  [metadata datatype]
  (when-let [ddl (:zero-one.geni/spark-type metadata)]
    (when-let [dt (try (DataType/fromDDL ddl) (catch Exception _ nil))]
      (when (contains? (recorded-datatypes dt) datatype)
        (->nullable dt)))))

(defn- ->spark-type-of
  "The Spark type that the :schema option gives a column: a DataType, a DDL
  string, or what `->schema` takes."
  ^DataType [col-name spec]
  (let [dt (cond
             (instance? DataType spec) spec
             (string? spec)            (DataType/fromDDL spec)
             :else                     (let [t (->schema spec)]
                                         (if (instance? DataType t) t (->spark-type t))))]
    (or dt
        (throw (ex-info (str "create-dataframe's :schema gives the column \"" col-name "\" "
                             (pr-str spec) ", which isn't a Spark type. It takes a DataType, a "
                             "DDL string such as \"DECIMAL(12, 2)\", or what g/->schema takes.")
                        {:column col-name :type spec})))))

;; Values, as their column's type has them

(defn- unrepresentable!
  [col-name ^DataType dt value]
  (throw (ex-info (str "The column \"" col-name "\" has the value " (pr-str value) ", which "
                       (.sql dt) " can't hold exactly. Give the column a type that holds it, "
                       "with create-dataframe's :schema, or convert the value first.")
                  {:column col-name :value value :type (.sql dt)})))

(defn- exact-decimal
  "A number as a BigDecimal: a whole number or a BigDecimal as it is, and a
  float or a double as its shortest decimal, as Spark's `Decimal` reads a
  double, or nil for anything else, an infinity or a NaN."
  ^BigDecimal [value]
  (cond
    (instance? Double value) (when (Double/isFinite value) (BigDecimal/valueOf (double value)))
    (instance? Float value)  (when (Float/isFinite value) (BigDecimal. (Float/toString value)))
    :else                    (->big-decimal value)))

(defn- decimal-value
  "`value` as a BigDecimal at the DECIMAL's scale, when the DECIMAL holds it
  exactly."
  [col-name ^DecimalType dt value]
  (let [d      (exact-decimal value)
        fitted (when d
                 (try (.setScale d (.scale dt) RoundingMode/UNNECESSARY)
                      (catch ArithmeticException _ nil)))]
    (if (and fitted (<= (.precision ^BigDecimal fitted) (.precision dt)))
      fitted
      (unrepresentable! col-name dt value))))

(defn- whole-value
  "`value`, a whole number between `lo` and `hi`, through `coerce`."
  [col-name dt value lo hi coerce]
  (if (and (integer? value) (<= lo value hi))
    (coerce value)
    (unrepresentable! col-name dt value)))

(defn- float-value
  "A number as a float, when it's within a float's range, or not finite."
  [col-name dt value]
  (let [d (when (number? value) (double value))]
    (if (and d (or (not (Double/isFinite d)) (<= (Math/abs (double d)) Float/MAX_VALUE)))
      (unchecked-float d)
      (unrepresentable! col-name dt value))))

(def ^:private micros-per-unit
  "The microseconds in each of a day-time interval's fields, by its index."
  [86400000000 3600000000 60000000 1])

(defn- whole-micros?
  "Whether the Duration is a whole number of microseconds that a long holds,
  as Spark keeps a day-time interval."
  [^Duration d]
  (and (zero? (rem (.getNano d) 1000))
       (try
         (Math/addExact (Math/multiplyExact (.getSeconds d) 1000000) (quot (.getNano d) 1000))
         true
         (catch ArithmeticException _ false))))

(defn- day-time-value
  "A Duration that the day-time interval holds exactly: to its last field,
  to the microsecond, and within its range."
  [col-name ^DayTimeIntervalType dt value]
  (if (and (instance? Duration value)
           (whole-micros? value)
           (let [unit (micros-per-unit (int (.endField dt)))]
             (or (= 1 unit)
                 (and (zero? (.getNano ^Duration value))
                      (zero? (rem (.getSeconds ^Duration value) (quot unit 1000000)))))))
    value
    (unrepresentable! col-name dt value)))

(defn- year-month-value
  "A Period that the year-month interval holds exactly: with no days, and in
  whole years for INTERVAL YEAR."
  [col-name ^YearMonthIntervalType dt value]
  (if (and (instance? Period value)
           (zero? (.getDays ^Period value))
           (or (== 1 (.endField dt))
               (zero? (rem (.toTotalMonths ^Period value) 12))))
    value
    (unrepresentable! col-name dt value)))

(defn- converter
  "A function from a value in the column `col-name`, at the top or inside
  it, to what Spark takes for `dt`. A map becomes a Row for a struct, by its
  keys' names, and a Scala map for a map, which a Spark Connect client needs
  and classic Spark takes too. A number becomes one of the class that its
  type takes. A value that its type can't hold exactly throws."
  [col-name ^DataType dt]
  (let [guard (fn [f] (fn [value] (when (some? value) (f value))))]
    (cond
      (instance? DecimalType dt)
      (guard #(decimal-value col-name dt %))

      (instance? ByteType dt)
      (guard #(whole-value col-name dt % Byte/MIN_VALUE Byte/MAX_VALUE byte))

      (instance? ShortType dt)
      (guard #(whole-value col-name dt % Short/MIN_VALUE Short/MAX_VALUE short))

      (instance? IntegerType dt)
      (guard #(whole-value col-name dt % Integer/MIN_VALUE Integer/MAX_VALUE int))

      (instance? LongType dt)
      (guard #(whole-value col-name dt % Long/MIN_VALUE Long/MAX_VALUE long))

      (instance? FloatType dt)
      (guard #(float-value col-name dt %))

      (instance? DoubleType dt)
      (guard #(if (number? %) (double %) (unrepresentable! col-name dt %)))

      (instance? DayTimeIntervalType dt)
      (guard #(day-time-value col-name dt %))

      (instance? YearMonthIntervalType dt)
      (guard #(year-month-value col-name dt %))

      (instance? ArrayType dt)
      (let [f (converter col-name (.elementType ^ArrayType dt))]
        (guard #(if (coll? %) (mapv f %) %)))

      (instance? MapType dt)
      (let [kf (converter col-name (.keyType ^MapType dt))
            vf (converter col-name (.valueType ^MapType dt))]
        (guard #(if (map? %)
                  (let [m (java.util.HashMap.)]
                    (doseq [[k v] %] (.put m (kf k) (vf v)))
                    (JavaConverters/mapAsScalaMap m))
                  %)))

      (instance? StructType dt)
      (let [fields (mapv (fn [^StructField field]
                           [(.name field) (converter col-name (.dataType field))])
                         (.fields ^StructType dt))]
        (guard #(if (map? %)
                  (let [values (by-field-name %)]
                    (interop/->spark-row (mapv (fn [[n f]] (f (get values n))) fields)))
                  %)))

      :else
      (guard #(if-let [convert (value-conversions (class %))] (convert %) %)))))

(defn- column-type
  "The Spark type of a tech.ml.dataset column whose values, with `tmd-value`
  applied, are `values`, as `create-dataframe` says."
  ^DataType [col-name datatype metadata values override]
  (or override
      (recorded-type metadata datatype)
      (when-let [dt (tmd-type->spark-type datatype)]
        (if (= :decimal datatype) (decimal-type-for col-name 18 values) dt))
      (fit-decimals col-name
                    (infer-spark-type col-name (first (first-non-nil values)))
                    values)))

(defn- tmd->dataframe
  "A DataFrame of a tech.ml.dataset dataset, through rows on the driver."
  [spark dataset {:keys [schema] :as options}]
  (when-not (map? options)
    (throw (ex-info (str "create-dataframe takes a map of options after a dataset, such as "
                         "{:schema {:price \"DECIMAL(12, 2)\"}}. Got: " (pr-str options))
                    {:options options})))
  (let [columns   ((requiring-resolve 'tech.v3.dataset/columns) dataset)
        names     (mapv #(column-name (:name (meta %))) columns)
        overrides (into {} (map (fn [[k v]] [(column-name k) v])) schema)
        unknown   (remove (set names) (keys overrides))
        _         (when (seq unknown)
                    (throw (ex-info (str "create-dataframe's :schema names columns that the dataset "
                                         "doesn't have: " (pr-str (vec unknown)) ". It has "
                                         (pr-str names) ".")
                                    {:columns (vec unknown)})))
        values    (mapv (fn [column]
                          (let [datatype (:datatype (meta column))]
                            (mapv #(tmd-value datatype %) column)))
                        columns)
        types     (mapv (fn [col-name column column-values]
                          (column-type col-name
                                       (:datatype (meta column))
                                       (meta column)
                                       column-values
                                       (some->> (get overrides col-name) (->spark-type-of col-name))))
                        names
                        columns
                        values)
        converted (mapv (fn [col-name dt column-values]
                          (mapv (converter col-name dt) column-values))
                        names
                        types
                        values)
        rows      (if (seq columns) (apply map vector converted) [])]
    (.createDataFrame spark
                      (interop/->java-list (map interop/->spark-row rows))
                      (DataTypes/createStructType
                       ^java.util.List
                       (mapv #(DataTypes/createStructField %1 %2 true) names types)))))

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

