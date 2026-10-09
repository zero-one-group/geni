(ns zero-one.geni.ml.tuning
  (:require
   [zero-one.geni.docs :as docs]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.utils :refer [import-fn]])
  (:import
   (org.apache.spark.ml.tuning ParamGridBuilder)))

(interop/def-stages org.apache.spark.ml.tuning
  [cross-validator CrossValidator]
  [train-validation-split TrainValidationSplit])

(defn param-grid-builder
  "An array of param maps, the grid's every combination, for a tuning
  stage's `:estimator-param-maps`, from a map of stages to maps of params to
  the values to try.

  ```clojure
  (ml/param-grid {log-reg {:reg-param [0.1 0.01] :max-iter [10 20]}})
  ```"
  [grids]
  (let [builder (ParamGridBuilder.)]
    (doseq [[stage grid] grids
            [k values]   grid
            :let         [[param values] (interop/grid-param stage k values)]]
      (.addGrid builder param (interop/->scala-seq values)))
    (.build builder)))

(defn avg-metrics
  "A cross-validator model's metric for each of its estimator's param maps,
  averaged over the folds, in the order of `:estimator-param-maps`."
  [model]
  (vec (.avgMetrics model)))

(defn validation-metrics
  "A train-validation split model's metric for each of its estimator's param
  maps, in the order of `:estimator-param-maps`."
  [model]
  (vec (.validationMetrics model)))

(defn sub-models
  "The models that a cross-validator or a train-validation split fitted for
  each param map, with `:collect-sub-models true`: a vector per fold of a
  model per param map for a cross-validator, and a model per param map for a
  split. Nil when it didn't keep them."
  [model]
  (when (.hasSubModels model)
    (mapv #(if (.isArray (class %)) (vec %) %) (.subModels model))))

;; Docs
(docs/alter-docs-in-ns!
 'zero-one.geni.ml.tuning
 [(-> docs/spark-docs :classes :ml :tuning)])

;; Aliases
(import-fn param-grid-builder param-grid)

