(ns zero-one.geni.ml
  (:refer-clojure :exclude [Double])
  (:require
    [camel-snake-kebab.core :refer [->kebab-case]]
    [clojure.walk :refer [keywordize-keys]]
    [potemkin :refer [import-vars]]
    [zero-one.geni.ml-classification]
    [zero-one.geni.ml-feature]
    [zero-one.geni.ml-regression]
    [zero-one.geni.scala :as scala])
  (:import
    (org.apache.spark.ml Pipeline PipelineStage)
    (org.apache.spark.ml.stat ChiSquareTest
                              Correlation)))

(import-vars
  [zero-one.geni.ml-feature
   binariser
   binarizer
   bucketiser
   bucketizer
   bucketed-random-projection-lsh
   count-vectoriser
   count-vectorizer
   dct
   discrete-cosine-transform
   elementwise-product
   feature-hasher
   hashing-tf
   idf
   imputer
   index-to-string
   interaction
   max-abs-scaler
   min-hash-lsh
   min-max-scaler
   n-gram
   normaliser
   one-hot-encoder
   one-hot-encoder-estimator
   pca
   polynomial-expansion
   quantile-discretiser
   quantile-discretiser
   sql-transformer
   standard-scaler
   string-indexer
   tokeniser
   tokenizer
   vector-assembler
   vector-indexer
   vector-size-hint
   word2vec])

(import-vars
  [zero-one.geni.ml-classification
   decision-tree-classifier
   gbt-classifier
   linear-svc
   logistic-regression
   multilayer-perceptron-classifier
   mlp-classifier
   naive-bayes
   one-vs-rest ;; TODO: MulticlassClassificationEvaluator
   random-forest-classifier])

(import-vars
  [zero-one.geni.ml-regression
   aft-survival-regression
   decision-tree-regressor
   gbt-regressor ;; TODO: investigate validationTol
   generalised-linear-regression
   generalized-linear-regression
   glm
   isotonic-regression
   linear-regression
   random-forest-regressor])

(defn corr [dataframe col-name]
  (Correlation/corr dataframe col-name))

(defn chi-square-test [dataframe features-col label-col]
  (ChiSquareTest/test dataframe features-col label-col))

(defn pipeline [& stages]
  (-> (Pipeline.)
      (.setStages (into-array PipelineStage stages))))

(defn fit [dataframe estimator]
  (.fit estimator dataframe))

(defn transform [dataframe transformer]
  (.transform transformer dataframe))

(defn params [stage]
  (let [param-pairs (-> stage .extractParamMap .toSeq scala/scala-seq->vec)
        unpack-pair (fn [p] [(-> p .param .name ->kebab-case) (.value p)])]
    (->> param-pairs
         (map unpack-pair)
         (into {})
         keywordize-keys)))

(defn vector->seq [spark-vector]
  (-> spark-vector .values seq))

(defn matrix->seqs [matrix]
  (let [n-cols (.numCols matrix)]
    (->> matrix .values seq (partition n-cols))))

(comment

  (int-array [1])

  (require '[zero-one.geni.core :as g])
  (require '[zero-one.geni.dataset :as ds])


  (defonce libsvm-df
    (-> @g/spark
        .read
        (.format "libsvm")
        (.load "test/resources/sample_libsvm_data.txt")))

  (g/print-schema libsvm-df)

  (import '(org.apache.spark.ml.regression IsotonicRegression))
  (-> (IsotonicRegression.)
      params)

  (use 'zero-one.geni.ml :reload)

  (use '[clojure.tools.namespace.repl :only (refresh)])
  (refresh)

  true)
