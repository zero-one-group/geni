;; Docstring Sources:
;; - https://github.com/apache/spark/blob/v3.0.1/sql/catalyst/src/main/java/org/apache/spark/sql/types/DataTypes.java
(ns zero-one.geni.core.dataset-creation
  (:require
    [zero-one.geni.defaults :as defaults]
    [zero-one.geni.docs :as docs]
    [zero-one.geni.interop :as interop])
  (:import
    (org.apache.spark.sql.types ArrayType DataType DataTypes)
    (org.apache.spark.ml.linalg VectorUDT
                                DenseVector
                                SparseVector)))

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
  (DataTypes/createArrayType
    (data-type->spark-type val-type)
    nullable))

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

(defn- infer-spark-type [value]
  (cond
    (coll? value) (ArrayType. (infer-spark-type (first value)) true)
    :else (get java-type->spark-type (type value) DataTypes/BinaryType)))

(defn- infer-struct-field [col-name value]
  (let [spark-type   (infer-spark-type value)]
    (DataTypes/createStructField col-name spark-type true)))

(defn- infer-schema [col-names values]
  (DataTypes/createStructType
    (mapv infer-struct-field col-names values)))

(defn- first-non-nil [values]
  (first (filter identity values)))

(defn- transpose [xs]
  (apply map list xs))

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
     (let [col-names (map name col-names)
           values    (map first-non-nil (transpose table))
           rows      (interop/->java-list (map interop/->spark-row table))
           schema    (infer-schema col-names values)]
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

;; Docs
(docs/add-doc!
  (var create-dataframe)
  (-> docs/spark-docs :methods :spark :session :create-data-frame))
