(ns zero-one.geni.rdd
  (:refer-clojure :exclude [count
                            distinct
                            empty?
                            filter
                            first
                            group-by
                            keys
                            map
                            mapcat
                            max
                            min
                            name
                            reduce
                            take
                            vals])
  (:require
    [potemkin :refer [import-vars]]
    [zero-one.geni.defaults]
    [zero-one.geni.interop :as interop]
    [zero-one.geni.rdd.function :as function]
    [zero-one.geni.storage])
  (:import
    (org.apache.spark.partial PartialResult)
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
(defn context [rdd] (JavaSparkContext/fromSparkContext (.context rdd)))

(defn id [rdd] (.id rdd))

(defn is-checkpointed [rdd] (.isCheckpointed rdd))
(def checkpointed? is-checkpointed)

(defn is-empty [rdd] (.isEmpty rdd))
(def empty? is-empty)

(defn name [rdd] (.name rdd))

(defn num-partitions [rdd] (.getNumPartitions rdd))

; TODO: must be able to test the use of partitioner
;(defn partitioner [rdd]
;  (let [maybe-partitioner (.partitioner rdd)]
;    (when (.isPresent maybe-partitioner)
;      (.get maybe-partitioner))))

(defn partitions [rdd] (.partitions rdd))

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
(def mapcat flat-map)

(defn flat-map-to-pair [rdd f]
  (.flatMapToPair rdd (function/pair-flat-map-function f)))
(def mapcat-to-pair flat-map-to-pair)

(defn glom [rdd]
  (.glom rdd))

(defn group-by-key
  ([rdd] (.groupByKey rdd))
  ([rdd num-partitions] (.groupByKey rdd num-partitions)))

(defn intersection [left right]
  (.intersection left right))

(defn key-by [rdd f]
  (.keyBy rdd (function/function f)))

(defmulti map (fn [head & _] (class head)))
(defmethod map :default [rdd f]
  (.map rdd (function/function f)))
(defmethod map PartialResult [result f]
  (.map result (interop/->scala-function1 f)))

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

(defn repartition [rdd num-partitions]
  (.repartition rdd num-partitions))

(defn sample
  ([rdd with-replacement fraction] (.sample rdd with-replacement fraction))
  ([rdd with-replacement fraction seed] (.sample rdd with-replacement fraction seed)))

(defn subtract
  ([left right] (.subtract left right))
  ([left right num-partitions] (.subtract left right num-partitions)))

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

;; Actions
(defn count [rdd]
  (.count rdd))

(defn- bounded-double->map [bounded-double]
  {:mean (.mean bounded-double)
   :confidence (.confidence bounded-double)
   :low (.low bounded-double)
   :high (.high bounded-double)})

(defn count-approx
  ([rdd timeout] (-> rdd (.countApprox timeout) (map bounded-double->map)))
  ([rdd timeout confidence] (-> rdd (.countApprox timeout confidence) (map bounded-double->map))))

(defn count-approx-distinct [rdd relative-sd]
  (.countApproxDistinct rdd relative-sd))

(defn count-async [rdd]
  (.countAsync rdd))

(defn count-by-value [rdd]
  (into {} (.countByValue rdd)))

(defn collect [rdd]
  (-> rdd .collect seq interop/->clojure))

(defn collect-async [rdd]
  (future (-> rdd .collectAsync deref seq interop/->clojure)))

(defn collect-partitions [rdd partition-ids]
  (->> (.collectPartitions rdd (int-array partition-ids))
       (clojure.core/map (comp interop/->clojure seq))))

(defn first [rdd]
  (.first rdd))

(defn foreach [rdd f]
  (.foreach rdd (function/void-function f)))

(defn foreach-async [rdd f]
  (.foreachAsync rdd (function/void-function f)))

(defn foreach-partition [rdd f]
  (.foreachPartition rdd (function/void-function f)))

(defn foreach-partition-async [rdd f]
  (.foreachPartitionAsync rdd (function/void-function f)))

(defn take [rdd n]
  (-> rdd (.take n) seq interop/->clojure))

(defn take-async [rdd n]
  (.takeAsync rdd n))

(defn take-ordered
  ([rdd n] (-> rdd (.takeOrdered n) seq interop/->clojure))
  ([rdd n cmp] (-> rdd (.takeOrdered n cmp) seq interop/->clojure)))

(defn take-sample
  ([rdd with-replacement n]
   (-> rdd (.takeSample with-replacement n) seq interop/->clojure))
  ([rdd with-replacement n seed] (.takeSample rdd with-replacement n seed)
   (-> rdd (.takeSample with-replacement n) seq interop/->clojure)))

(defn save-as-text-file [rdd path]
  (.saveAsTextFile rdd path))

;; PairRDD Transformations
(defn count-by-key [rdd]
  (into {} (.countByKey rdd)))

(defn flat-map-values [rdd f]
  (.flatMapValues rdd (function/flat-map-function f)))

(defn full-outer-join
  ([left right] (.fullOuterJoin left right))
  ([left right num-partitions] (.fullOuterJoin left right num-partitions)))

(defn group-by
  ([rdd f] (.groupBy rdd (function/function f)))
  ([rdd f num-partitions] (.groupBy rdd (function/function f) num-partitions)))

(defn join
  ([left right] (.join left right))
  ([left right num-partitions] (.join left right num-partitions)))

(defn keys [rdd]
  (.keys rdd))

(defn left-outer-join
  ([left right] (.leftOuterJoin left right))
  ([left right num-partitions] (.leftOuterJoin left right num-partitions)))

(defn map-values [rdd f]
  (.mapValues rdd (function/function f)))

(defn reduce-by-key [rdd f]
  (.reduceByKey rdd (function/function2 f)))

(defn right-outer-join
  ([left right] (.rightOuterJoin left right))
  ([left right num-partitions] (.rightOuterJoin left right num-partitions)))

(defn sort-by-key
  ([rdd] (.sortByKey rdd))
  ([rdd asc] (.sortByKey rdd asc)))

(defn subtract-by-key
  ([left right] (.subtractByKey left right))
  ([left right num-partitions] (.subtractByKey left right num-partitions)))

(defn values [rdd]
  (.values rdd))
(def vals values)

;; PairRDD Actions
(defn lookup [rdd k]
  (seq (.lookup rdd k)))

;; Partial Result
(defn final-value [result] (.getFinalValue result))

(defn initial-value [result] (.initialValue result))

(defn is-initial-value-final [result] (.isInitialValueFinal result))
(def final? is-initial-value-final)

;; Polymorphic

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

;; RDD Transformations
;; TODO: aggregate, fold
;; Others:
;; TODO: broadcast
;; TODO: name unmangling / setting callsite name?
;; PairRDD
;; TODO: aggregate-by-key, cogroup, cogroup-partitioned, combine-by-key
;; TODO: pipe, repartition-and-sort-within-partitions
