(ns zero-one.geni.dataset
  (:require
    [zero-one.geni.scala :as scala])
  (:import
    (org.apache.spark.sql RowFactory)
    (org.apache.spark.sql.types DataTypes)
    (org.apache.spark.ml.linalg VectorUDT)))

(defn ->row [coll]
  (RowFactory/create (into-array Object (map scala/->scala-coll coll))))

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
   nil                           DataTypes/NullType
   clojure.lang.PersistentVector (VectorUDT.)})

(defn infer-struct-field [col-name value]
  (let [default-type DataTypes/BinaryType
        java-type    (type value)
        spark-type   (get java-type->spark-type java-type default-type)]
    (DataTypes/createStructField col-name spark-type true)))

(defn infer-schema [col-names values]
  (DataTypes/createStructType
    (mapv infer-struct-field col-names values)))

(defn first-non-nil [values]
  (first (filter identity values)))

(defn transpose [xs]
  (apply map list xs))

(defn table->dataset [spark table col-names]
  (let [
        col-names (map name col-names)
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
  (let [col-names     (-> (map keys records) flatten distinct)
        map-of-values (reduce
                        conj-record
                        (zipmap col-names (repeat []))
                        records)]
    (map->dataset spark map-of-values)))

(comment
  true)
