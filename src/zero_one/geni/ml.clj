(ns zero-one.geni.ml
  (:refer-clojure :exclude [Double])
  (:require
    [camel-snake-kebab.core :refer [->kebab-case]]
    [clojure.walk :refer [keywordize-keys]]
    [potemkin :refer [import-vars]]
    [zero-one.geni.ml-classification]
    [zero-one.geni.ml-clustering]
    [zero-one.geni.ml-evaluation]
    [zero-one.geni.ml-feature]
    [zero-one.geni.ml-fpm]
    [zero-one.geni.ml-recommendation]
    [zero-one.geni.ml-regression]
    [zero-one.geni.ml-tuning]
    [zero-one.geni.ml-xgb]
    [zero-one.geni.interop :as interop])
  (:import
    (org.apache.spark.ml Pipeline
                         PipelineStage)
    (org.apache.spark.ml.stat ChiSquareTest
                              Correlation)))

(import-vars
  [zero-one.geni.ml-clustering
   bisecting-k-means
   gaussian-mixture
   gmm
   k-means
   lda
   latent-dirichlet-allocation])

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
   chi-sq-selector
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
   regex-tokeniser
   regex-tokenizer
   sql-transformer
   standard-scaler
   string-indexer
   stop-words-remover
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
  [zero-one.geni.ml-fpm
   fp-growth
   frequent-pattern-growth
   prefix-span])

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

(import-vars
  [zero-one.geni.ml-recommendation
   als
   alternating-least-squares
   recommend-for-all-items
   recommend-for-all-users
   recommend-for-item-subset
   recommend-for-user-subset
   recommend-items
   recommend-users])

(import-vars
  [zero-one.geni.ml-tuning
   param-grid
   cross-validator
   train-validation-split])

(import-vars
  [zero-one.geni.ml-xgb
   xgboost-classifier
   xgboost-regressor])

(defn corr [dataframe col-name]
  (Correlation/corr dataframe col-name))
(def correlation corr)

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

(defn approx-nearest-neighbours
  ([dataset model key-v n-nearest]
   (.approxNearestNeighbors model dataset (interop/->scala-coll key-v) n-nearest))
  ([dataset model key-v n-nearest dist-col]
   (.approxNearestNeighbors model dataset (interop/->scala-coll key-v) n-nearest dist-col)))
(defn approx-similarity-join
  ([dataset-a dataset-b model threshold]
   (.approxSimilarityJoin model dataset-a dataset-b threshold))
  ([dataset-a dataset-b model threshold dist-col]
   (.approxSimilarityJoin model dataset-a dataset-b threshold dist-col)))
(defn association-rules [model] (.associationRules model))
(defn binary-summary [model] (.binarySummary model))
(defn best-model [model] (.bestModel model))
(defn boundaries [model] (interop/->clojure (.boundaries model)))
(defn category-maps [model]
  (->> model
       .categoryMaps
       interop/scala-map->map
       (map (fn [[k v]] [k (interop/scala-map->map v)]))
       (into {})))
(defn category-sizes [model] (seq (.categorySizes model)))
(defn cluster-centers [model] (->> model .clusterCenters seq (map interop/->clojure)))
(defn coefficient-matrix [model] (interop/matrix->seqs (.coefficientMatrix model)))
(defn coefficients [model] (interop/vector->seq (.coefficients model)))
(defn compute-cost [dataset model] (.computeCost model dataset))
(defn depth [model] (.depth model))
(def describe-topics (memfn describeTopics))
(defn estimated-doc-concentration [model] (interop/->clojure (.estimatedDocConcentration model)))
(defn feature-importances [model] (interop/->clojure (.featureImportances model)))
(defn find-frequent-sequential-patterns [dataset prefix-span]
  (.findFrequentSequentialPatterns prefix-span dataset))
(def find-patterns find-frequent-sequential-patterns)
(defn frequent-item-sets [model] (.freqItemsets model))
(def freq-itemsets frequent-item-sets)
(defn gaussians-df [model] (.gaussiansDF model))
(defn get-features-col [model] (.getFeaturesCol model))
(def features-col get-features-col)
(defn get-input-col [model] (.getInputCol model))
(def input-col get-input-col)
(defn get-input-cols [model] (seq (.getInputCols model)))
(def input-cols get-input-cols)
(defn get-label-col [model] (.getLabelCol model))
(def label-col get-label-col)
(defn get-output-col [model] (.getOutputCol model))
(def output-col get-output-col)
(defn get-output-cols [model] (seq (.getOutputCols model)))
(def output-cols get-output-cols)
(defn get-prediction-col [model] (.getPredictionCol model))
(def prediction-col get-prediction-col)
(defn get-probability-col [model] (.getProbabilityCol model))
(def probability-col get-probability-col)
(defn get-raw-prediction-col [model] (.getRawPredictionCol model))
(def raw-prediction-col get-raw-prediction-col)
(defn get-thresholds [model] (seq (.getThresholds model)))
(def thresholds get-thresholds)
(defn get-num-trees [model] (.getNumTrees model))
(defn get-size [model] (.getSize model))
(defn idf-vector [model] (interop/vector->seq (.idf model)))
(defn intercept [model] (.intercept model))
(defn intercept-vector [model] (interop/vector->seq (.interceptVector model)))
(defn is-distributed [model] (.isDistributed model))
(def distributed? is-distributed)
(defn log-likelihood [dataset model] (.logLikelihood model dataset))
(defn log-perplexity [dataset model] (.logPerplexity model dataset))
(defn max-abs [model] (interop/vector->seq (.maxAbs model)))
(defn mean [model] (interop/vector->seq (.mean model)))
(defn num-classes [model] (.numClasses model))
(defn num-features [model] (.numFeatures model))
(defn num-nodes [model] (.numNodes model))
(defn original-max [model] (interop/vector->seq (.originalMax model)))
(defn original-min [model] (interop/vector->seq (.originalMin model)))
(defn pc [model] (interop/matrix->seqs (.pc model)))
(def principal-components pc)
(defn pi [model] (interop/vector->seq (.pi model)))
(defn root-node [model] (.rootNode model))
(defn scale [model] (.scale model))
(defn summary [model] (.summary model))
(defn supported-optimizers [model] (seq (.supportedOptimizers model)))
(def supported-optimisers supported-optimizers)
(defn stages [model] (seq (.stages model)))
(defn std [model] (interop/vector->seq (.std model)))
(defn surrogate-df [model] (.surrogateDF model))
(defn theta [model] (interop/matrix->seqs (.theta model)))
(defn total-num-nodes [model] (.totalNumNodes model))
(defn tree-weights [model] (seq (.treeWeights model)))
(defn trees [model] (seq (.trees model)))
(defn uid [model] (.uid model))
(defn vocab-size [model] (.vocabSize model))
(defn vocabulary [model] (seq (.vocabulary model)))
(defn weights [model] (seq (.weights model)))

(defn write-stage! [model path]
  (.. model
      write
      overwrite
      (save path)))

(defn load-method? [^java.lang.reflect.Method method]
  (and ; (= 1 (alength ^"[Ljava.lang.Class;" (.getParameterTypes method)))
       (= "load" (.getName method))))

(defn load-method [cls]
  (->> cls
       .getMethods
       (filter load-method?)
       first))

(defn read-stage! [model-cls path]
  (.invoke (load-method model-cls) model-cls (into-array [path])))

(comment

  (import '(org.apache.spark.ml.feature StopWordsRemover))
  (stop-words-remover {})

  (params (StopWordsRemover.))

  true)
