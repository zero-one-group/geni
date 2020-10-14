(ns zero-one.geni.defaults
  (:require
    [zero-one.geni.spark]))

(def spark
  "The default SparkSession as a Delayed object."
  (delay
    (zero-one.geni.spark/create-spark-session
      {:configs {:spark.sql.adaptive.enabled "true"
                 :spark.sql.adaptive.coalescePartitions.enabled "true"}
       :checkpoint-dir "target/checkpoint/"})))
