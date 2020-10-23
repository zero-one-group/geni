(ns zero-one.geni.ml.evaluation
  (:require
   [zero-one.geni.docs :as docs]
   [zero-one.geni.interop :as interop])
  (:import
   (org.apache.spark.ml.evaluation BinaryClassificationEvaluator
                                   ClusteringEvaluator
                                   MulticlassClassificationEvaluator
                                   MultilabelClassificationEvaluator
                                   RankingEvaluator
                                   RegressionEvaluator)))

(defn binary-classification-evaluator [params]
  (interop/instantiate BinaryClassificationEvaluator params))

(defn clustering-evaluator [params]
  (interop/instantiate ClusteringEvaluator params))

(defn multiclass-classification-evaluator [params]
  (interop/instantiate MulticlassClassificationEvaluator params))

(defn multilabel-classification-evaluator [params]
  (interop/instantiate MultilabelClassificationEvaluator params))

(defn ranking-evaluator [params]
  (interop/instantiate RankingEvaluator params))

(defn regression-evaluator [params]
  (interop/instantiate RegressionEvaluator params))

;; Docs
(docs/alter-docs-in-ns!
 'zero-one.geni.ml.evaluation
 [(-> docs/spark-docs :classes :ml :evaluation)])

