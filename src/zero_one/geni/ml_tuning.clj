(ns zero-one.geni.ml-tuning
  (:require
    [zero-one.geni.interop :as interop])
  (:import
    (org.apache.spark.ml.tuning CrossValidator ParamGridBuilder)))

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

(defn cross-validator [{:keys [estimator evaluator estimator-param-maps num-folds seed parallelism]}]
  (-> (CrossValidator.)
      (cond-> estimator (.setEstimator estimator))
      (cond-> evaluator (.setEvaluator evaluator))
      (cond-> estimator-param-maps (.setEstimatorParamMaps estimator-param-maps))
      (cond-> num-folds (.setNumFolds num-folds))
      (cond-> seed (.setSeed seed))
      (cond-> parallelism (.setParallelism parallelism))))
