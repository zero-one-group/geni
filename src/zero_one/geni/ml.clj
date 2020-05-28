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
    [zero-one.geni.interop :as interop])
  (:import
    (org.apache.spark.ml Pipeline
                         PipelineStage)
    (org.apache.spark.ml.stat ChiSquareTest
                              Correlation)))

(import-vars
  [zero-one.geni.ml-clustering
   bisecting-k-means
   cluster-centers
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

;; TODO: describe-topics (LDA), computeCost (BisectingKMeans)
;; TODO: gaussians (GaussianMixtureModel)
;; TODO: turn summary into maps
(defn association-rules [model] (.associationRules model))
(defn binary-summary [model] (.binarySummary model))
;(defn boundaries [model] (interop/->clojure (.boundaries model)))
;(defn cluster-centers [model] (.clusterCenters model))
(defn coefficient-matrix [model] (interop/matrix->seqs (.coefficientMatrix model)))
(defn coefficients [model] (interop/vector->seq (.coefficients model)))
(defn depth [model] (.depth model))
;(defn estimated-doc-concentration [model] (interop/->clojure (.estimatedDocConcentration model)))
(defn feature-importances [model] (interop/->clojure (.featureImportances model)))
(defn find-frequent-sequential-patterns [dataset prefix-span]
  (.findFrequentSequentialPatterns prefix-span dataset))
(def find-patterns find-frequent-sequential-patterns)
(defn frequent-item-sets [model] (.freqItemsets model))
(def freq-itemsets frequent-item-sets)
;(defn gaussians-df [model] (.gaussiansDF model))
(defn get-num-trees [model] (.getNumTrees model))
(defn intercept [model] (.intercept model))
(defn intercept-vector [model] (interop/vector->seq (.interceptVector model)))
;(defn is-distributed [model] (.isDistributed model))
;(def distributed? is-distributed)
;(defn log-likelihood [model] (.logLikelihood model))
;(defn log-perplexity [model] (.logPerplexity model))
(defn num-classes [model] (.numClasses model))
(defn num-features [model] (.numFeatures model))
(defn num-nodes [model] (.numNodes model))
;(defn pi [model] (interop/->clojure (.pi model)))
(defn root-node [model] (.rootNode model))
;(defn scale [model] (.scale model))
(defn summary [model] (.summary model))
;(defn theta [model] (interop/->clojure (.theta model)))
(defn total-num-nodes [model] (.totalNumNodes model))
(defn tree-weights [model] (seq (.treeWeights model)))
(defn trees [model] (seq (.trees model)))
(defn uid [model] (.uid model))
;(defn vocab-size [model] (.vocabSize model))

;; TODO: read-stage
(defn write-stage! [model path]
  (.. model
      write
      overwrite
      (save path)))

(comment

  (require '[zero-one.geni.core :as g])
  (require '[zero-one.geni.dataset :as ds])
  (require '[zero-one.geni.test-resources :refer [spark libsvm-df k-means-df]])

  (g/print-schema libsvm-df)
  (g/print-schema k-means-df)

  (def model
    (let [estimator   (k-means {:k 3})
          model       (fit k-means-df estimator)]
      model))


  (.toPMML model "temp.xml")

  (.toPMML model)

  (class model)

  (require '[clojure.reflect :as r])
  (->> (r/reflect model)
       :members
       (mapv :name)
       sort)


  (import '(org.apache.spark.ml.fpm PrefixSpan))
  (params (PrefixSpan.))

  true)
