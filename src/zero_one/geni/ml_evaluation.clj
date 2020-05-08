(ns zero-one.geni.ml-evaluation
  (:require
    [zero-one.geni.interop :as interop])
  (:import
    (org.apache.spark.ml.evaluation BinaryClassificationEvaluator
                                    ClusteringEvaluator
                                    MulticlassClassificationEvaluator
                                    RegressionEvaluator)))

(defn binary-classification-evaluator [params]
  (interop/instantiate BinaryClassificationEvaluator params))

(defn clustering-evaluator [params]
  (interop/instantiate ClusteringEvaluator params))

(defn multiclass-classification-evaluator [params]
  (interop/instantiate MulticlassClassificationEvaluator params))

(defn regression-evaluator [params]
  (interop/instantiate RegressionEvaluator params))

