(ns zero-one.geni.defaults
  (:require
   [zero-one.geni.spark]
   [zero-one.geni.interop :as iop]))

(def session-config
  {:configs (merge {:spark.sql.adaptive.enabled "true"
                    :spark.sql.adaptive.coalescePartitions.enabled "true"}
                   (when (iop/class-exists? 'io.delta.sql.DeltaSparkSessionExtension)
                     ;; Required for writing Delta tables.
                     {:spark.sql.extensions "io.delta.sql.DeltaSparkSessionExtension"
                      :spark.sql.catalog.spark_catalog "org.apache.spark.sql.delta.catalog.DeltaCatalog"}))
   :checkpoint-dir "target/checkpoint/"})

(def spark
  "The default SparkSession as a Delayed object."
  (atom
   (zero-one.geni.spark/create-spark-session session-config)))
