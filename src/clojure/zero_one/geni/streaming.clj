(ns zero-one.geni.streaming
  (:import
    (org.apache.spark.streaming Milliseconds
                                Minutes
                                Seconds
                                StreamingContext)
    (org.apache.spark.sql SparkSession)))

(defn milliseconds [t] (Milliseconds/apply t))

(defn minutes [t] (Minutes/apply t))

(defn seconds [t] (Seconds/apply t))

(defmulti streaming-context (fn [head & _] (class head)))
(defmethod streaming-context SparkSession [spark duration]
  (StreamingContext. (.sparkContext spark) duration))
