(ns {{namespace}}.core-test
  (:require
    [clojure.test :refer [deftest is]]
    [{{namespace}}.core :as core]
    [zero-one.geni.core :as g])
  (:import
    (org.apache.spark.sql Dataset SparkSession)))

(deftest correct-dataset-instance
  (is (instance? Dataset @core/training-set)))

(deftest correct-spark-session-instance
  (is (instance? SparkSession @core/spark)))

(deftest correct-spark-config
  (is (= (-> @core/spark g/spark-conf :spark.master) "local[*]")))
