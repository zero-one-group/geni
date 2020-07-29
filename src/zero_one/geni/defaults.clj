(ns zero-one.geni.defaults
  (:require
    [zero-one.geni.spark]))

(def spark
  (delay
    (zero-one.geni.spark/create-spark-session
      {:configs {:spark.sql.adaptive.enabled "true"
                 :spark.sql.adaptive.coalescePartitions.enabled "true"}
       :checkpoint-dir "target/checkpoint/"})))
