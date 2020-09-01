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
    [zero-one.geni.rdd.function :as function]
    [zero-one.geni.storage])
  (:import
    (org.apache.spark.partial PartialResult)
    (org.apache.spark.api.java JavaSparkContext)
    (org.apache.spark.sql SparkSession)))

(def value (memfn value))

;; Java Spark Context
(defn java-spark-context [spark]
  (JavaSparkContext/fromSparkContext (.sparkContext spark)))

(defn app-name [spark]
  (-> spark java-spark-context .appName))

(defn broadcast [spark value]
  (-> spark java-spark-context (.broadcast value)))

(defn checkpoint-dir [spark]
  (interop/optional->nillable (-> spark java-spark-context .getCheckpointDir)))

(defn conf [spark]
  (-> spark java-spark-context .getConf interop/spark-conf->map))

(defn default-min-partitions [spark]
  (-> spark java-spark-context .defaultMinPartitions))

(defn default-parallelism [spark]
  (-> spark java-spark-context .defaultParallelism))

(defn empty-rdd [spark]
  (-> spark java-spark-context .emptyRDD))

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

(defn partitioner [rdd]
  (interop/optional->nillable (.partitioner rdd)))

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

;; Partial Result
(defn final-value [result] (.getFinalValue result))

(defn initial-value [result] (.initialValue result))

(defn is-initial-value-final [result] (.isInitialValueFinal result))
(def final? is-initial-value-final)

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

;; JavaSparkContext:
;; TODO: checkpoint-dir, spark-conf, local-property, persistent-rdds, spark-home
;; TODO: local?, jars, master, parallelise-doubles, parallelise-pairs, resources,
;; TODO: spark-context/sc, union, version

;; Others:
;; TODO: name unmangling / setting callsite name
