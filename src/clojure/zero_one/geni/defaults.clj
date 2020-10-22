(ns zero-one.geni.defaults
  (:require
   [zero-one.geni.spark]))

(def session-config
  {:configs {:spark.sql.adaptive.enabled "true"
             :spark.sql.adaptive.coalescePartitions.enabled "true"}
   :checkpoint-dir "target/checkpoint/"})

(def spark
  "The default SparkSession as a Delayed object."
  (atom
   (zero-one.geni.spark/create-spark-session session-config)))
