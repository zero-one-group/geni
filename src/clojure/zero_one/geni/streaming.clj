(ns zero-one.geni.streaming
  (:refer-clojure :exclude [count
                            filter
                            map
                            print
                            reduce])
  (:require
   [potemkin :refer [import-vars]]
   [zero-one.geni.rdd.function :as function]
   [zero-one.geni.storage]
   [zero-one.geni.spark-context :as spark-context])
  (:import
   (org.apache.spark.streaming.api.java JavaDStream JavaStreamingContext)
   (org.apache.spark.streaming Duration
                               Milliseconds
                               Minutes
                               Seconds
                               Time)))

;; TODO: count-by-value-and-window,
;; TODO: foreachRDD, reduce-by-window
;; TODO: slice, transform, transform-to-pair, transform-with,
;; TODO: transform-with-to-pair, window

(defn milliseconds [t] (Milliseconds/apply t))

(defn minutes [t] (Minutes/apply t))

(defn seconds [t] (Seconds/apply t))

(defn ->duration [value]
  (if (instance? Duration value)
    value
    (milliseconds value)))

;; StreamingContext
(defn streaming-context [spark duration]
  (JavaStreamingContext.
   (spark-context/java-spark-context spark)
   (->duration duration)))

(defn await-termination! [context]
  (future (.awaitTermination context)))

(defn await-termination-or-timeout! [context timeout]
  (future (.awaitTerminationOrTimeout context timeout)))

; TODO: how to test without killing the SparkContext?
; (def close (memfn close))

(defn remember [context duration]
  (.remember context (->duration duration)))

(defmulti save-as-text-files! (fn [head & _] (class head)))
(defmethod save-as-text-files! JavaDStream [dstream path]
  (save-as-text-files! (.dstream dstream) path))
(defmethod save-as-text-files! :default [dstream path]
  (.saveAsTextFiles dstream path ""))

(defn socket-text-stream [context hostname port storage]
  (.socketTextStream context hostname port storage))

(defn spark-context [context]
  (.sparkContext context))

(defn ssc [context]
  (.ssc context))

(defn start! [context]
  (future (.start context)))

(defn state [context]
  (.getState context))
(def get-state state)

(defn stop! [context]
  (future (.stop context false true)))

(defn text-file-stream [context path]
  (.textFileStream context path))

;; DStream
(defn cache [dstream]
  (.cache dstream))

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

(defn map [dstream f]
  (.map dstream (function/function f)))

(defn map-partitions [dstream f]
  (.mapPartitions dstream (function/flat-map-function f)))

(defn map-partitions-to-pair [dstream f]
  (.mapPartitionsToPair dstream (function/pair-flat-map-function f)))

(defn map-to-pair [dstream f]
  (.mapToPair dstream (function/pair-function f)))

(defn persist
  ([dstream] (.persist dstream))
  ([dstream storage-level] (.persist dstream storage-level)))

(defn print
  ([dstream] (.print dstream))
  ([dstream num] (.print dstream num)))

(defn reduce [dstream f]
  (.reduce dstream (function/function2 f)))

(defn repartition [dstream num-partitions]
  (.repartition dstream num-partitions))

(defn slide-duration [dstream]
  (.slideDuration dstream))

(defn union [left right]
  (.union left right))

(defn wrap-rdd [dstream rdd]
  (.wrapRDD dstream (.rdd rdd)))

;; PairDStream
(defn ->java-dstream [dstream]
  (.toJavaDStream dstream))

(defn reduce-by-key
  ([dstream f]
   (.reduceByKey dstream (function/function2 f)))
  ([dstream f partitions-or-partitioner]
   (.reduceByKey dstream (function/function2 f) partitions-or-partitioner)))

;; StorageLevel

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

;; Polymorphic
(defmulti checkpoint (fn [head & _] (class head)))
(defmethod checkpoint JavaStreamingContext [context directory]
  (.checkpoint context directory))
(defmethod checkpoint :default [dstream interval]
  (.checkpoint dstream interval))
