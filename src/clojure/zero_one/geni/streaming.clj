(ns zero-one.geni.streaming
  (:refer-clojure :exclude [count
                            print])
  (:require
    [potemkin :refer [import-vars]]
    [zero-one.geni.rdd.function :as function]
    [zero-one.geni.storage]
    [zero-one.geni.spark-context :as spark-context])
  (:import
    (org.apache.spark.streaming.api.java JavaDStream JavaStreamingContext)
    (org.apache.spark.streaming Milliseconds
                                Minutes
                                Seconds)
    (org.apache.spark.sql SparkSession)))

(defn milliseconds [t] (Milliseconds/apply t))

(defn minutes [t] (Minutes/apply t))

(defn seconds [t] (Seconds/apply t))

(defmulti streaming-context (fn [head & _] (class head)))
(defmethod streaming-context SparkSession [spark duration]
  (JavaStreamingContext. (spark-context/java-spark-context spark) duration))

(defn socket-text-stream [context hostname port storage]
  (.socketTextStream context hostname port storage))

(defn text-file-stream [context path]
  (.textFileStream context path))

(defmulti save-as-text-files! (fn [head & _] (class head)))
(defmethod save-as-text-files! JavaDStream [d-stream path]
  (save-as-text-files! (.dstream d-stream) path))
(defmethod save-as-text-files! :default [d-stream path]
  (.saveAsTextFiles d-stream path ""))

(defn start! [context]
  (future (.start context)))

(defn await-termination! [context]
  (future (.awaitTermination context)))

(defn stop! [context]
  (future (.stop context false true)))

(defn cache [d-stream]
  (.cache d-stream))

(defn checkpoint [d-stream interval]
  (.checkpoint d-stream interval))

(defn context [d-stream]
  (.context d-stream))

(defn count [d-stream]
  (.count d-stream))

(defn flat-map [d-stream f]
  (.flatMap d-stream (function/flat-map-function f)))

(defn glom [d-stream]
  (.glom d-stream))

(defn persist
  ([d-stream] (.persist d-stream))
  ([d-stream storage-level] (.persist d-stream storage-level)))

(defn print
  ([d-stream] (.print d-stream))
  ([d-stream num] (.print d-stream num)))

(defn slide-duration [d-stream]
  (.slideDuration d-stream))

(defn union [left right]
  (.union left right))

(import-vars
  [zero-one.geni.storage
   disk-only
   disk-only-2
   memory-and-disk
   memory-and-disk-2
   memory-and-disk-ser
   memory-and-disk-ser-2
   memory-only
   memory-only-2
   memory-only-ser
   memory-only-ser-2
   none
   off-heap])
