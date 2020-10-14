(ns zero-one.geni.core.dataset-creation
  (:require
    [zero-one.geni.defaults]
    [zero-one.geni.interop :as interop])
  (:import
    (org.apache.spark.sql.types ArrayType DataType DataTypes)
    (org.apache.spark.ml.linalg VectorUDT
                                DenseVector
                                SparseVector)))

(def default-spark zero-one.geni.defaults/spark)

(def data-type->spark-type
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

(defn struct-field [col-name data-type nullable]
  (let [spark-type (if (instance? DataType data-type)
                     data-type
                     (data-type->spark-type data-type))]
    (DataTypes/createStructField (name col-name) spark-type nullable)))

(defn struct-type [& fields]
  (DataTypes/createStructType fields))

(defn array-type [val-type nullable]
  (DataTypes/createArrayType
    (data-type->spark-type val-type)
    nullable))

(defn map-type [key-type val-type]
  (DataTypes/createMapType
    (data-type->spark-type key-type)
    (data-type->spark-type val-type)))

(defn ->schema [value]
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

(defn empty-schema? [schema]
  (if (coll? schema)
    (empty? schema)
    false))

(defn create-dataframe
  ([rows schema] (create-dataframe @default-spark rows schema))
  ([spark rows schema]
   (if (and (empty? rows) (empty-schema? schema))
     (.emptyDataFrame spark)
     (.createDataFrame spark rows (->schema schema)))))

(def java-type->spark-type
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

(defn infer-spark-type [value]
  (cond
    (coll? value) (ArrayType. (infer-spark-type (first value)) true)
    :else (get java-type->spark-type (type value) DataTypes/BinaryType)))

(defn infer-struct-field [col-name value]
  (let [spark-type   (infer-spark-type value)]
    (DataTypes/createStructField col-name spark-type true)))

(defn infer-schema [col-names values]
  (DataTypes/createStructType
    (mapv infer-struct-field col-names values)))

(defn first-non-nil [values]
  (first (filter identity values)))

(defn transpose [xs]
  (apply map list xs))

(defn table->dataset
  ([table col-names] (table->dataset @default-spark table col-names))
  ([spark table col-names]
   (if (empty? table)
     (.emptyDataFrame spark)
     (let [col-names (map name col-names)
           values    (map first-non-nil (transpose table))
           rows      (interop/->java-list (map interop/->spark-row table))
           schema    (infer-schema col-names values)]
       (.createDataFrame spark rows schema)))))

(defn map->dataset
  ([map-of-values] (map->dataset @default-spark map-of-values))
  ([spark map-of-values]
   (if (empty? map-of-values)
     (.emptyDataFrame spark)
     (let [table     (transpose (vals map-of-values))
           col-names (keys map-of-values)]
       (table->dataset spark table col-names)))))

(defn conj-record [map-of-values record]
  (let [col-names (keys map-of-values)]
    (reduce
      (fn [acc-map col-name]
        (update acc-map col-name #(conj % (get record col-name))))
      map-of-values
      col-names)))

(defn records->dataset
  ([records] (records->dataset @default-spark records))
  ([spark records]
   (let [col-names     (-> (map keys records) flatten distinct)
         map-of-values (reduce
                         conj-record
                         (zipmap col-names (repeat []))
                         records)]
     (map->dataset spark map-of-values))))
