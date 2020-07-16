(ns zero-one.geni.dataset-creation
  (:require
    [zero-one.geni.interop :as interop])
  (:import
    (org.apache.spark.sql.types ArrayType DataTypes)
    (org.apache.spark.ml.linalg VectorUDT
                                DenseVector
                                SparseVector)))

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
  (let [spark-type (data-type->spark-type data-type)]
    (DataTypes/createStructField (name col-name) spark-type nullable)))

(defn struct-type [& fields]
  (DataTypes/createStructType fields))

(defn create-dataframe [spark rows schema]
  (.createDataFrame spark rows schema))

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
  (first (clojure.core/filter identity values)))

(defn transpose [xs]
  (apply map list xs))

(defn table->dataset [spark table col-names]
  (let [col-names (map name col-names)
        values    (map first-non-nil (transpose table))
        rows      (interop/->java-list (map interop/->spark-row table))
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
