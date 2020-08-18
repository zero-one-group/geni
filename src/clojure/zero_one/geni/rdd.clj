(ns zero-one.geni.rdd
  (:refer-clojure :exclude [distinct
                            filter
                            map
                            reduce])
  (:require
    [zero-one.geni.defaults]
    [zero-one.geni.interop :as interop]
    [zero-one.geni.rdd.function :as function])
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

;; Transformations
(defn distinct [rdd]
  (.distinct rdd))

(defn filter [rdd f]
  (.filter rdd (function/function f)))

(defn flat-map [rdd f]
  (.flatMap rdd (function/flat-map-function f)))

(defn intersection [left right]
  (.intersection left right))

(defn map [rdd f]
  (.map rdd (function/function f)))

(defn map-to-pair [rdd f]
  (.mapToPair rdd (function/pair-function f)))

(defn reduce [rdd f]
  (.reduce rdd (function/function2 f)))

(defn reduce-by-key [rdd f]
  (.reduceByKey rdd (function/function2 f)))

(defn union [left right]
  (.union left right))

;; TODO: map-partitions, map-partitions-with-index
;; TODO: intersection, distinct, group-by-key, sort-by-key, join, coalesce

;; Actions
(defn collect [rdd] (-> rdd .collect seq interop/->clojure))

(comment

  (ns zero-one.geni.rdd)
  (require '[zero-one.geni.aot-functions :as aot])
  (def lines (text-file "test/resources/rdd.txt"))
  (-> (parallelise  [["Four score and seven years ago our fathers"]
                     ["brought forth on this continent a new nation"]])
      (flat-map function/split-spaces)
      collect)

  (require '[clojure.pprint])
  (require '[clojure.reflect :as r])
  (->> (r/reflect lines)
      :members
      clojure.pprint/pprint)

  true)

