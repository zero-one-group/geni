(ns zero-one.geni.ml.evaluation
  (:require
   [zero-one.geni.docs :as docs]
   [zero-one.geni.interop :as interop]))

(interop/def-stages org.apache.spark.ml.evaluation
  [binary-classification-evaluator BinaryClassificationEvaluator]
  [clustering-evaluator ClusteringEvaluator]
  [multiclass-classification-evaluator MulticlassClassificationEvaluator]
  [multilabel-classification-evaluator MultilabelClassificationEvaluator]
  [ranking-evaluator RankingEvaluator]
  [regression-evaluator RegressionEvaluator])

;; Docs
(docs/alter-docs-in-ns!
 'zero-one.geni.ml.evaluation
 [(-> docs/spark-docs :classes :ml :evaluation)])

