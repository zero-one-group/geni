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
                            partition-by
                            reduce
                            take
                            vals])
  (:require
    [potemkin :refer [import-vars]]
    [zero-one.geni.defaults]
    [zero-one.geni.interop :as interop]
    [zero-one.geni.partial-result]
    [zero-one.geni.rdd.function :as function]
    [zero-one.geni.spark-context]
    [zero-one.geni.storage])
  (:import
    (org.apache.spark.partial PartialResult)
    (org.apache.spark.api.java JavaSparkContext)))

(import-vars
  [zero-one.geni.spark-context
   app-name
   binary-files
   broadcast
   checkpoint-dir
   conf
   default-min-partitions
   default-parallelism
   empty-rdd
   is-local
   jars
   java-spark-context
   local-property
   local?
   master
   parallelise
   parallelise-doubles
   parallelise-pairs
   parallelize
   parallelize-doubles
   parallelize-pairs
   persistent-rdds
   resources
   sc
   spark-context
   spark-home
   text-file
   value
   version])

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

(import-vars
  [zero-one.geni.partial-result
   final-value
   final?
   initial-value
   is-initial-value-final])

;; Getters
(defn context [rdd] (JavaSparkContext/fromSparkContext (.context rdd)))

(defn id [rdd] (.id rdd))

(defn is-checkpointed [rdd] (.isCheckpointed rdd))
(def checkpointed? is-checkpointed)

(defn is-empty [rdd] (.isEmpty rdd))
(def empty? is-empty)

(defn name [rdd] (.name rdd))

(defn num-partitions [rdd] (.getNumPartitions rdd))

(defn partitioner [rdd]
  (interop/optional->nillable (.partitioner rdd)))

(defn partitions [rdd] (.partitions rdd))

(defn storage-level [rdd] (.getStorageLevel rdd))

;; Transformations
(defn aggregate [rdd zero seq-op comb-op]
  (.aggregate rdd zero (function/function2 seq-op) (function/function2 comb-op)))

(defn cache [rdd]
  (.cache rdd))

(defn cartesian [left right]
  (.cartesian left right))

(defn coalesce
  ([rdd num-partitions] (.coalesce rdd num-partitions))
  ([rdd num-partitions shuffle] (.coalesce rdd num-partitions shuffle)))

(defn cogroup
  ([this other1] (.cogroup this other1))
  ([this other1 other2] (.cogroup this other1 other2))
  ([this other1 other2 other3] (.cogroup this other1 other2 other3)))

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

(defn fold [rdd zero f]
  (.fold rdd zero (function/function2 f)))

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

(defn map-partitions-to-pair
  ([rdd f] (map-partitions rdd f false))
  ([rdd f preserves-partitioning]
   (.mapPartitionsToPair rdd (function/pair-flat-map-function f) preserves-partitioning)))

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

(defn repartition-and-sort-within-partitions
  ([rdd partitioner] (.repartitionAndSortWithinPartitions rdd partitioner))
  ([rdd partitioner cmp] (.repartitionAndSortWithinPartitions rdd partitioner cmp)))

(defn sample
  ([rdd with-replacement fraction] (.sample rdd with-replacement fraction))
  ([rdd with-replacement fraction seed] (.sample rdd with-replacement fraction seed)))

(defn subtract
  ([left right] (.subtract left right))
  ([left right partitions-or-partitioner]
   (.subtract left right partitions-or-partitioner)))

(defn top
  ([rdd n] (-> rdd (.top n) seq interop/->clojure))
  ([rdd n cmp] (-> rdd (.top n cmp) seq interop/->clojure)))

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
(defn aggregate-by-key
  ([rdd zero seq-fn comb-fn]
   (.aggregateByKey rdd
                    zero
                    (function/function2 seq-fn)
                    (function/function2 comb-fn)))
  ([rdd zero num-partitions seq-fn comb-fn]
   (.aggregateByKey rdd
                    num-partitions
                    zero
                    (function/function2 seq-fn)
                    (function/function2 comb-fn))))

(defn combine-by-key
  ([rdd create-fn merge-value-fn merge-combiner-fn]
   (.combineByKey rdd
                  (function/function create-fn)
                  (function/function2 merge-value-fn)
                  (function/function2 merge-combiner-fn)))
  ([rdd create-fn merge-value-fn merge-combiner-fn partitions-or-partitioner]
   (.combineByKey rdd
                  (function/function create-fn)
                  (function/function2 merge-value-fn)
                  (function/function2 merge-combiner-fn)
                  partitions-or-partitioner)))

(defn count-by-key [rdd]
  (into {} (.countByKey rdd)))

(defn bounded-double-map->nested-map [m]
  (into {} (for [[k v] m] [k (bounded-double->map v)])))

(defn count-by-key-approx
  ([rdd timeout] (count-by-key-approx rdd timeout 0.95))
  ([rdd timeout confidence]
   (-> rdd
       (.countByKeyApprox timeout confidence)
       (map bounded-double-map->nested-map))))

(defn count-approx-distinct-by-key
  ([rdd relative-sd] (.countApproxDistinctByKey rdd relative-sd))
  ([rdd relative-sd partitions-or-partitioner]
   (.countApproxDistinctByKey rdd relative-sd partitions-or-partitioner)))

(defn flat-map-values [rdd f]
  (.flatMapValues rdd (function/flat-map-function f)))

(defn fold-by-key
  ([rdd zero f] (.foldByKey rdd zero (function/function2 f)))
  ([rdd zero partitions-or-partitioner f]
   (.foldByKey rdd zero partitions-or-partitioner (function/function2 f))))

(defn full-outer-join
  ([left right] (.fullOuterJoin left right))
  ([left right partitions-or-partitioner] (.fullOuterJoin left right partitions-or-partitioner)))

(defn group-by
  ([rdd f] (.groupBy rdd (function/function f)))
  ([rdd f num-partitions] (.groupBy rdd (function/function f) num-partitions)))

(defn join
  ([left right] (.join left right))
  ([left right partitions-or-partitioner] (.join left right partitions-or-partitioner)))

(defn keys [rdd]
  (.keys rdd))

(defn left-outer-join
  ([left right] (.leftOuterJoin left right))
  ([left right partitions-or-partitioner]
   (.leftOuterJoin left right partitions-or-partitioner)))

(defn map-values [rdd f]
  (.mapValues rdd (function/function f)))

(defn partition-by [rdd partitioner]
  (.partitionBy rdd partitioner))

(defn reduce-by-key
  ([rdd f] (.reduceByKey rdd (function/function2 f)))
  ([rdd f partitions-or-partitioner]
   (.reduceByKey rdd (function/function2 f) partitions-or-partitioner)))

(defn reduce-by-key-locally [rdd f]
  (into {} (.reduceByKeyLocally rdd (function/function2 f))))

(defn right-outer-join
  ([left right] (.rightOuterJoin left right))
  ([left right partitions-or-partitioner]
   (.rightOuterJoin left right partitions-or-partitioner)))

(defn sample-by-key
  ([rdd with-replacement fractions]
   (.sampleByKey rdd with-replacement fractions))
  ([rdd with-replacement fractions seed]
   (.sampleByKey rdd with-replacement fractions seed)))

(defn sample-by-key-exact
  ([rdd with-replacement fractions]
   (.sampleByKeyExact rdd with-replacement fractions))
  ([rdd with-replacement fractions seed]
   (.sampleByKeyExact rdd with-replacement fractions seed)))

(defn sort-by-key
  ([rdd] (.sortByKey rdd))
  ([rdd asc] (.sortByKey rdd asc)))

(defn subtract-by-key
  ([left right] (.subtractByKey left right))
  ([left right partitions-or-partitioner]
   (.subtractByKey left right partitions-or-partitioner)))

(defn values [rdd]
  (.values rdd))
(def vals values)

;; PairRDD Actions
(defn lookup [rdd k]
  (seq (.lookup rdd k)))

