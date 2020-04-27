(ns zero-one.geni.dataset
  (:import
    (org.apache.spark.sql RowFactory)
    (org.apache.spark.sql.types DataTypes)))

(defn ->row [coll]
  (RowFactory/create (into-array Object coll)))

(defn ->java-list [coll]
  (java.util.ArrayList. coll))

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
   nil                DataTypes/NullType})

(defn infer-struct-field [col-name value]
  (let [default-type DataTypes/BinaryType
        java-type    (type value)
        spark-type   (get java-type->spark-type java-type default-type)]
    (DataTypes/createStructField col-name spark-type true)))

(defn infer-schema [col-names values]
  (DataTypes/createStructType
    (mapv infer-struct-field col-names values)))

(defn transpose [xs]
  (apply map list xs))

;; TODO: add test
;; TODO: implement better schema inference.
(defn map->dataset [spark map-of-values]
  (let [col-names  (map name (keys map-of-values))
        values     (map first (vals map-of-values))
        row-values (-> map-of-values vals transpose)
        rows       (->java-list (map ->row row-values))
        schema     (infer-schema col-names values)]
    (.createDataFrame spark rows schema)))

(comment

  (require '[zero-one.geni.core :as g])
  (defonce spark
    (delay (g/create-spark-session {:configs {"spark.testing.memory" "2147480000"}})))

  (def map-of-values
    {:a [1 4]
     :b [2.0 5.0]
     :c ["a" "b"]})

  (g/print-schema
    (map->dataset
      @spark
      map-of-values))

  ;; TODO: DF creation should be easy
  (records->dataset
    spark
    [{:a 1 :b 2.0 :c "a"}
     {:a 4 :b 5.0 :c "b"}])

  (table->dataset
    spark
    [[1 2.0 "a"]
     [4 5.0 "b"]]
    [:a :b :c])


  true)
