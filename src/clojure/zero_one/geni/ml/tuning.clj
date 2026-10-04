(ns zero-one.geni.ml.tuning
  (:require
   [zero-one.geni.utils :refer [import-fn]]
   [zero-one.geni.docs :as docs]
   [zero-one.geni.interop :as interop])
  (:import
   (org.apache.spark.ml.tuning CrossValidator
                               ParamGridBuilder
                               TrainValidationSplit)))

(defn param-grid-builder [grids]
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

(defn cross-validator [{:keys [estimator evaluator estimator-param-maps num-folds seed parallelism
                               collect-sub-models]}]
  (-> (CrossValidator.)
      (cond-> (some? collect-sub-models) (.setCollectSubModels (boolean collect-sub-models)))
      (cond-> estimator (.setEstimator estimator))
      (cond-> evaluator (.setEvaluator evaluator))
      (cond-> estimator-param-maps (.setEstimatorParamMaps estimator-param-maps))
      (cond-> num-folds (.setNumFolds num-folds))
      (cond-> seed (.setSeed seed))
      (cond-> parallelism (.setParallelism parallelism))))

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

(defn train-validation-split [{:keys [estimator evaluator estimator-param-maps seed parallelism
                                      collect-sub-models train-ratio]}]
  (-> (TrainValidationSplit.)
      (cond-> train-ratio (.setTrainRatio (double train-ratio)))
      (cond-> (some? collect-sub-models) (.setCollectSubModels (boolean collect-sub-models)))
      (cond-> estimator (.setEstimator estimator))
      (cond-> evaluator (.setEvaluator evaluator))
      (cond-> estimator-param-maps (.setEstimatorParamMaps estimator-param-maps))
      (cond-> seed (.setSeed seed))
      (cond-> parallelism (.setParallelism parallelism))))

;; Docs
(docs/alter-docs-in-ns!
 'zero-one.geni.ml.tuning
 [(-> docs/spark-docs :classes :ml :tuning)])

;; Aliases
(import-fn param-grid-builder param-grid)

