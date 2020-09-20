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

(defn text-file-stream [context path]
  (.textFileStream context path))

(defn save-as-text-files! [d-stream path]
  (.saveAsTextFiles d-stream path ""))

(defn start! [context]
  (future (.start context)))

(defn await-termination! [context]
  (future (.awaitTermination context)))

(defn stop! [context]
  (future (.stop context true true)))
