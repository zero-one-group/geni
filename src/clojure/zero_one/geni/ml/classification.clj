(ns zero-one.geni.ml.classification
  (:require
   [zero-one.geni.docs :as docs]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.utils :refer [import-fn]]))

(interop/def-stages org.apache.spark.ml.classification
  [decision-tree-classifier DecisionTreeClassifier]
  [fm-classifier FMClassifier]
  [gbt-classifier GBTClassifier]
  [linear-svc LinearSVC]
  [logistic-regression LogisticRegression]
  [multilayer-perceptron-classifier MultilayerPerceptronClassifier]
  [naive-bayes NaiveBayes]
  [one-vs-rest OneVsRest]
  [random-forest-classifier RandomForestClassifier])

;; Docs
(docs/alter-docs-in-ns!
 'zero-one.geni.ml.classification
 [(-> docs/spark-docs :classes :ml :classification)])

;; Aliases
(import-fn multilayer-perceptron-classifier mlp-classifier)

