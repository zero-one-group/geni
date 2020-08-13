(ns zero-one.geni.rdd.core
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

(defmacro sfn [& body]
  `(sfn/fn ~@body))

(defn map [rdd f]
  (.map rdd (function/function (sfn [x] (f x)))))

(defn reduce [rdd f]
  (.reduce rdd (function/function2 (sfn [x y] (f x y)))))

(defn collect [rdd] (-> rdd .collect seq))

(comment

  (require '[zero-one.geni.rdd.function :as function])

  (def default-sc (java-spark-context @zero-one.geni.defaults/spark))

  (parallelise [1 2 3 4 5])

  (def lines (text-file "test/resources/rdd.txt"))

  (text-file "test/resources/rdd.txt" 10)

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

