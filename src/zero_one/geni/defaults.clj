(ns zero-one.geni.defaults
  (:require
    [zero-one.geni.core :as g]))

(def spark
  (future
    (g/create-spark-session
      {:configs {:spark.testing.memory "3147480000"
                 :spark.sql.adaptive.enabled "true"
                 :spark.sql.adaptive.coalescePartitions.enabled "true"}
       :checkpoint-dir "resources/checkpoint/"})))
