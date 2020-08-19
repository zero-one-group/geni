(ns zero-one.geni.rdd
  (:refer-clojure :exclude [count
                            distinct
                            filter
                            map
                            max
                            min
                            reduce])
  (:require
    [potemkin :refer [import-vars]]
    [zero-one.geni.defaults]
    [zero-one.geni.interop :as interop]
    [zero-one.geni.rdd.function :as function]
    [zero-one.geni.storage])
  (:import
    (org.apache.spark.api.java JavaSparkContext)
    (org.apache.spark.sql SparkSession)))

(defn java-spark-context [spark]
  (JavaSparkContext/fromSparkContext (.sparkContext spark)))

(defn parallelise
  ([data] (parallelise @zero-one.geni.defaults/spark data))
  ([spark data] (-> spark java-spark-context (.parallelize data))))
(def parallelize parallelise)

(defmulti text-file (fn [head & _] (class head)))
(defmethod text-file :default
  ([path] (text-file @zero-one.geni.defaults/spark path))
  ([path min-partitions] (text-file @zero-one.geni.defaults/spark path min-partitions)))
(defmethod text-file SparkSession
  ([spark path] (-> spark java-spark-context (.textFile path)))
  ([spark path min-partitions] (-> spark java-spark-context (.textFile path min-partitions))))

;; Getters
(defn num-partitions [rdd] (.getNumPartitions rdd))

(defn storage-level [rdd] (.getStorageLevel rdd))

;; Transformations
(defn cache [rdd]
  (.cache rdd))

(defn cartesian [left right]
  (.cartesian left right))

(defn coalesce
  ([rdd num-partitions] (.coalesce rdd num-partitions))
  ([rdd num-partitions shuffle] (.coalesce rdd num-partitions shuffle)))

(defn distinct
  ([rdd] (.distinct rdd))
  ([rdd num-partitions] (.distinct rdd num-partitions)))

(defn filter [rdd f]
  (.filter rdd (function/function f)))

(defn flat-map [rdd f]
  (.flatMap rdd (function/flat-map-function f)))

(defn flat-map-to-pair [rdd f]
  (.flatMapToPair rdd (function/pair-flat-map-function f)))

(defn group-by-key
  ([rdd] (.groupByKey rdd))
  ([rdd num-partitions] (.groupByKey rdd num-partitions)))

(defn intersection [left right]
  (.intersection left right))

(defn key-by [rdd f]
  (.keyBy rdd (function/function f)))

(defn map [rdd f]
  (.map rdd (function/function f)))

(defn map-partitions
  ([rdd f] (map-partitions rdd f false))
  ([rdd f preserves-partitioning]
   (.mapPartitions rdd (function/flat-map-function f) preserves-partitioning)))

(defn map-partitions-with-index
  ([rdd f] (map-partitions-with-index rdd f false))
  ([rdd f preserves-partitioning]
   (.mapPartitionsWithIndex rdd (function/function2 f) preserves-partitioning)))

(defn map-to-pair [rdd f]
  (.mapToPair rdd (function/pair-function f)))

(defn max [rdd cmp]
  (.max rdd cmp))

(defn min [rdd cmp]
  (.min rdd cmp))

(defn persist [rdd storage]
  (.persist rdd storage))

(defn random-split
  ([rdd weights] (seq (.randomSplit rdd (double-array weights))))
  ([rdd weights seed] (seq (.randomSplit rdd (double-array weights) seed))))

(defn reduce [rdd f]
  (.reduce rdd (function/function2 f)))

(defn reduce-by-key [rdd f]
  (.reduceByKey rdd (function/function2 f)))

(defn repartition [rdd num-partitions]
  (.repartition rdd num-partitions))

(defn sample
  ([rdd with-replacement fraction] (.sample rdd with-replacement fraction))
  ([rdd with-replacement fraction seed] (.sample rdd with-replacement fraction seed)))

(defn subtract
  ([left right] (.subtract left right))
  ([left right num-partitions] (.subtract left right num-partitions)))

(defn sort-by-key
  ([rdd] (.sortByKey rdd))
  ([rdd asc] (.sortByKey rdd asc)))

(defn union [left right]
  (.union left right))

(defn unpersist
  ([rdd] (.unpersist rdd))
  ([rdd blocking] (.unpersist rdd blocking)))

(defn zip [left right]
  (.zip left right))

(defn zip-partitions [left right f]
  (.zipPartitions left right (function/flat-map-function2 f)))

(defn zip-with-index [rdd]
  (.zipWithIndex rdd))

(defn zip-with-unique-id [rdd]
  (.zipWithUniqueId rdd))

;; PairRDD Transformations
;; TODO: aggregate-by-key
;; TODO: join, cogroup, pipe
;; TODO: repartition-and-sort-within-partitions

;; Actions
(defn count [rdd] (.count rdd))

(defn collect [rdd] (-> rdd .collect seq interop/->clojure))

(defn foreach [rdd f] (.foreach rdd (function/void-function f)))

;; TODO: aggregate, collect-async, collect-partitions, context, count-approx
;; TODO: count-approx-distinct, count-async, count-by-value, fold, fereach-async
;; TODO: foreach-partition, foreach-partition-async, glom
;; TODO: first, take, take-sample, take-ordered, save-as-text-file
;; TODO: save-as-object-file, count-by-key

;; Static
;; TODO: id, checkpointed?, empty?, name, partitioner, partitions
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

(comment

  (ns zero-one.geni.rdd)
  (require '[zero-one.geni.aot-functions :as aot])

  (-> (text-file "test/resources/rdd.txt")
      (map-to-pair aot/to-pair)
      (.groupByKey 10)
      .getNumPartitions)

  (require '[clojure.pprint])
  (require '[clojure.reflect :as r])
  (->> (r/reflect lines)
      :members
      clojure.pprint/pprint)

  true)

