(ns zero-one.geni.ml
  (:refer-clojure :exclude [Double])
  (:require
    [camel-snake-kebab.core :refer [->kebab-case]]
    [clojure.walk :refer [keywordize-keys]]
    [potemkin :refer [import-vars]]
    [zero-one.geni.ml-classification]
    [zero-one.geni.ml-evaluation]
    [zero-one.geni.ml-feature]
    [zero-one.geni.ml-regression]
    [zero-one.geni.interop :as interop])
  (:import
    (org.apache.spark.ml Pipeline PipelineStage)
    (org.apache.spark.ml.stat ChiSquareTest
                              Correlation)))

(import-vars
  [zero-one.geni.ml-evaluation
   binary-classification-evaluator
   clustering-evaluator
   multiclass-classification-evaluator
   regression-evaluator])

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
   normalizer
   one-hot-encoder
   one-hot-encoder-estimator
   pca
   polynomial-expansion
   quantile-discretiser
   quantile-discretizer
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
   one-vs-rest
   random-forest-classifier])

(import-vars
  [zero-one.geni.ml-regression
   aft-survival-regression
   decision-tree-regressor
   gbt-regressor
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

(defn evaluate [dataframe evaluator]
  (.evaluate evaluator dataframe))

(defn params [stage]
  (let [param-pairs (-> stage .extractParamMap .toSeq interop/scala-seq->vec)
        unpack-pair (fn [p]
                      [(-> p .param .name ->kebab-case) (interop/->clojure (.value p))])]
    (->> param-pairs
         (map unpack-pair)
         (into {})
         keywordize-keys)))

(comment

  (require '[zero-one.geni.core :as g])
  (require '[zero-one.geni.dataset :as ds])

  (defonce libsvm-df
    (-> @g/spark
        .read
        (.format "libsvm")
        (.load "test/resources/sample_libsvm_data.txt")))

  (import '(org.apache.spark.ml.evaluation RegressionEvaluator))
  (params (RegressionEvaluator.))

  (defn multiclass-classification-evaluator [params]
    (let [defaults {:label-col "label"
                    :metric-name "f1"
                    :prediction-col "prediction"}
          props    (merge defaults params)]
      (interop/instantiate MulticlassClassificationEvaluator props)))


  (let [estimator   (logistic-regression
                      {:max-iter 10
                       :reg-param 0.3
                       :elastic-net-param 0.8
                       :family "multinomial"})
        model       (fit libsvm-df estimator)
        predictions (-> libsvm-df
                        (transform model)
                        (g/select "prediction" "label" "features"))
        evaluator   (multiclass-classification-evaluator
                      {:label-col "label"
                       :prediction-col "prediction"
                       :metric-name "accuracy"})
        accuracy   (evaluate predictions evaluator)]
    accuracy)

  (g/print-schema libsvm-df)


  (import '(org.apache.spark.ml.evaluation RegressionEvaluator))

  (import '(org.apache.spark.ml.linalg DenseVector))
  (defn dense-vector? [value]
    (instance? DenseVector value))

  (-> (feature-hasher
        {:input-cols (interop/->scala-seq ["real" "bool" "stringNum" "string"])}))
      ;params
      ;:scaling-vec
      ;dense-vector?)


  (require '[clojure.java.data :as j])

  (def method (:input-cols (interop/setters-map FeatureHasher)))

  (= (interop/setter-type method) scala.collection.Seq)

  (j/to-java)


  (import '(org.apache.spark.ml.feature FeatureHasher))
  (-> (FeatureHasher.)
      (.setInputCols (into-array java.lang.String ["real" "bool" "stringNum" "string"])))

  true)
