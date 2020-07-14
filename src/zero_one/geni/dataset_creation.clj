(ns zero-one.geni.dataset-creation
  (:require
    [zero-one.geni.interop :as interop]
    [zero-one.geni.utils :refer [vector-of-doubles?]])
  (:import
    (org.apache.spark.sql RowFactory)
    (org.apache.spark.sql.types ArrayType DataTypes)
    (org.apache.spark.ml.linalg VectorUDT)))

;;;; Dataset Creation
(defn ->row [coll]
  (RowFactory/create (into-array Object (map interop/->scala-coll coll))))

(defn ->java-list [coll]
  (java.util.ArrayList. coll))

(def java-type->spark-type
  {java.lang.Boolean             DataTypes/BooleanType
   java.lang.Byte                DataTypes/ByteType
   java.lang.Double              DataTypes/DoubleType
   java.lang.Float               DataTypes/FloatType
   java.lang.Integer             DataTypes/IntegerType
   java.lang.Long                DataTypes/LongType
   java.lang.Short               DataTypes/ShortType
   java.lang.String              DataTypes/StringType
   java.sql.Timestamp            DataTypes/TimestampType
   java.util.Date                DataTypes/DateType
   nil                           DataTypes/NullType})

(defn infer-spark-type [value]
  (cond
    (vector-of-doubles? value) (VectorUDT.)
    (coll? value) (ArrayType. (infer-spark-type (first value)) true)
    :else (get java-type->spark-type (type value) DataTypes/BinaryType)))

(defn infer-struct-field [col-name value]
  (let [spark-type   (infer-spark-type value)]
    (DataTypes/createStructField col-name spark-type true)))

(defn infer-schema [col-names values]
  (DataTypes/createStructType
    (mapv infer-struct-field col-names values)))

(defn first-non-nil [values]
  (first (clojure.core/filter identity values)))

(defn transpose [xs]
  (apply map list xs))

(defn table->dataset [spark table col-names]
  (let [col-names (map name col-names)
        values    (map first-non-nil (transpose table))
        rows      (->java-list (map ->row table))
        schema    (infer-schema col-names values)]
    (.createDataFrame spark rows schema)))

(defn map->dataset [spark map-of-values]
  (let [table     (transpose (vals map-of-values))
        col-names (keys map-of-values)]
    (table->dataset spark table col-names)))

(defn conj-record [map-of-values record]
  (let [col-names (keys map-of-values)]
    (reduce
      (fn [acc-map col-name]
        (update acc-map col-name #(conj % (get record col-name))))
      map-of-values
      col-names)))

(defn records->dataset [spark records]
  (let [col-names     (-> (map keys records) flatten clojure.core/distinct)
        map-of-values (reduce
                        conj-record
                        (zipmap col-names (repeat []))
                        records)]
    (map->dataset spark map-of-values)))

