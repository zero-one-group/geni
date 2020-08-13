(ns zero-one.geni.rdd
  (:refer-clojure :exclude [map
                            reduce])
  (:require
    [serializable.fn :as sfn]
    [zero-one.geni.defaults]
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

(defn map [rdd f]
  (let [sfn (sfn/fn [x] (f x))]
    (.map rdd (function/function sfn))))

(defn reduce [rdd f]
  (let [sfn (sfn/fn [x y] (f x y))]
    (.reduce rdd (function/function2 sfn))))

(defn collect [rdd] (-> rdd .collect seq))

(comment

  (def default-sc (java-spark-context @zero-one.geni.defaults/spark))

  (parallelise [1 2 3 4 5])

  (text-file "test/resources/rdd.txt" 10)

  (def lines (text-file "test/resources/rdd.txt"))

  (-> lines
      (map count)
      collect)

  (-> lines
      (map count)
      (reduce +))

  (require '[clojure.pprint])
  (require '[clojure.reflect :as r])
  (->> (r/reflect lines)
      :members
      clojure.pprint/pprint)

  true)

