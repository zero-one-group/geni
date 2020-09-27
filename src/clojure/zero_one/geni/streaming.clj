(ns zero-one.geni.streaming
  (:refer-clojure :exclude [count
                            filter
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
                                Seconds
                                Time)
    (org.apache.spark.sql SparkSession)))

;; TODO: count-by-value-and-window,
;; TODO: foreachRDD, map, map-partitions-to-pair, reduce, reduce-by-window
;; TODO: repartition, slice, transform, transform-to-pair, transform-with,
;; TODO: transform-with-to-pair, window, wrap-rdd

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
(defmethod save-as-text-files! JavaDStream [dstream path]
  (save-as-text-files! (.dstream dstream) path))
(defmethod save-as-text-files! :default [dstream path]
  (.saveAsTextFiles dstream path ""))

(defn start! [context]
  (future (.start context)))

(defn await-termination! [context]
  (future (.awaitTermination context)))

(defn stop! [context]
  (future (.stop context false true)))

(defn cache [dstream]
  (.cache dstream))

(defn checkpoint [dstream interval]
  (.checkpoint dstream interval))

(defn context [dstream]
  (.context dstream))

(defn ->time [value]
  (if (instance? Time value) value (Time. value)))

(defn compute [dstream t]
  (.compute dstream (->time t)))

(defn count [dstream]
  (.count dstream))

(defn count-by-value
  ([dstream] (.countByValue dstream))
  ([dstream num-partitions] (.countByValue dstream num-partitions)))

(def dstream (memfn dstream))

(defn filter [dstream f]
  (.filter dstream (function/function f)))

(defn flat-map [dstream f]
  (.flatMap dstream (function/flat-map-function f)))

(defn flat-map-to-pair [dstream f]
  (.flatMapToPair dstream (function/pair-flat-map-function f)))
(def mapcat-to-pair flat-map-to-pair)

(defn flat-map-values [dstream f]
  (.flatMapValues dstream (function/flat-map-function f)))

(defn glom [dstream]
  (.glom dstream))

(defn map-to-pair [rdd f]
  (.mapToPair rdd (function/pair-function f)))

(defn persist
  ([dstream] (.persist dstream))
  ([dstream storage-level] (.persist dstream storage-level)))

(defn print
  ([dstream] (.print dstream))
  ([dstream num] (.print dstream num)))

(defn slide-duration [dstream]
  (.slideDuration dstream))

(defn union [left right]
  (.union left right))

;; Pair DStream
(defn ->java-dstream [dstream]
  (.toJavaDStream dstream))

(defn reduce-by-key
  ([dstream f]
   (.reduceByKey dstream (function/function2 f)))
  ([dstream f partitions-or-partitioner]
   (.reduceByKey dstream (function/function2 f) partitions-or-partitioner)))

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
