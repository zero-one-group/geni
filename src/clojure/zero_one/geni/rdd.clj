(ns zero-one.geni.rdd
  (:refer-clojure :exclude [map
                            reduce])
  (:require
    [zero-one.geni.defaults]
    [zero-one.geni.interop :as interop]
    [zero-one.geni.rdd.function :as function])
  (:import
    (scala Tuple2)
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
(defn map [rdd f]
  (.map rdd (function/function f)))

(defn reduce [rdd f]
  (.reduce rdd (function/function2 f)))

(defn map-to-pair [rdd f]
  (.mapToPair rdd (function/pair-function f)))

;; Actions
(defn collect [rdd] (-> rdd .collect seq interop/->clojure))

(comment

  (ns zero-one.geni.rdd)

  (require '[zero-one.geni.aot-functions :as aot])

  (def lines (text-file "test/resources/rdd.txt"))

  (-> lines
      (map-to-pair aot/to-pair)
      collect
      first)

  (require '[clojure.pprint])
  (require '[clojure.reflect :as r])
  (->> (r/reflect lines)
      :members
      clojure.pprint/pprint)

  true)

