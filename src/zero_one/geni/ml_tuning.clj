(ns zero-one.geni.ml-tuning
  (:require
    [zero-one.geni.interop :as interop])
  (:import
    (org.apache.spark.ml.tuning CrossValidator
                                ParamGridBuilder)))

(defn param-grid [grids]
  (let [builder (ParamGridBuilder.)]
    (doall
      (for [[stage grid-map] grids]
        (doall
          (for [[param-keyword grid] grid-map]
            (.addGrid
              builder
              (interop/get-field stage param-keyword)
              (interop/->scala-seq grid))))))
    (.build builder)))

(defn cross-validator [options])
