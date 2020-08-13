(ns zero-one.geni.rdd.core
  (:refer-clojure :exclude [map
                            reduce])
  (:require
    [zero-one.geni.defaults])
  (:import
    (org.apache.spark.api.java JavaSparkContext)
    (org.apache.spark.api.java.function Function
                                        Function2)
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

(defn ->function [f]
  (reify Function (call [_ x] (f x))))

(defn ->function2 [f]
  (reify Function2 (call [_ x y] (f x y))))

(defn map [rdd f] (.map rdd (->function f)))

(defn reduce [rdd f] (.reduce rdd (->function2 f)))

(comment

  (def default-sc (java-spark-context @zero-one.geni.defaults/spark))

  (parallelise [1 2 3 4 5])

  (def lines (text-file "test/resources/rdd.txt"))

  (text-file "test/resources/rdd.txt" 10)

  (.collect (map lines count))

  (reduce (map lines count) (fn [x y] (+ x y)))

  (reduce lines (fn [x y] 1))

  (require '[clojure.pprint])
  (require '[clojure.reflect :as r])
  (->> (r/reflect Function)
      :members
      ;(clojure.core/filter #(= (:name %) 'map))
      ;(mapv :parameter-types)
      ;(clojure.core/filter #(= (:name %) 'toDF))
      ;clojure.core/sort
      clojure.pprint/pprint)

  true)

