(ns zero-one.geni.ml
  (:refer-clojure :exclude [Double])
  (:require
    [camel-snake-kebab.core :refer [->kebab-case]]
    [clojure.walk :refer [keywordize-keys]]
    [potemkin :refer [import-fn import-vars]]
    [zero-one.geni.core.column :as column]
    [zero-one.geni.core.polymorphic :as polymorphic]
    [zero-one.geni.docs :as docs]
    [zero-one.geni.interop :as interop]
    [zero-one.geni.ml.classification]
    [zero-one.geni.ml.clustering]
    [zero-one.geni.ml.evaluation]
    [zero-one.geni.ml.feature]
    [zero-one.geni.ml.fpm]
    [zero-one.geni.ml.recommendation]
    [zero-one.geni.ml.regression]
    [zero-one.geni.ml.tuning]
    [zero-one.geni.ml.xgb])
  (:import
    (org.apache.spark.ml Pipeline
                         PipelineStage
                         functions)
    (org.apache.spark.ml.stat ChiSquareTest
                              KolmogorovSmirnovTest)))

(import-vars
  [zero-one.geni.ml.xgb
   write-native-model!
   xgboost-classifier
   xgboost-regressor])

(import-vars
  [zero-one.geni.ml.clustering
   bisecting-k-means
   gaussian-mixture
   gmm
   k-means
   lda
   latent-dirichlet-allocation
   power-iteration-clustering])

(import-vars
  [zero-one.geni.ml.evaluation
   binary-classification-evaluator
   clustering-evaluator
   multiclass-classification-evaluator
   multilabel-classification-evaluator
   ranking-evaluator
   regression-evaluator])

(import-vars
  [zero-one.geni.ml.feature
   binariser
   binarizer
   bucketed-random-projection-lsh
   bucketiser
   bucketizer
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
   pca
   polynomial-expansion
   quantile-discretiser
   quantile-discretizer
   regex-tokeniser
   regex-tokenizer
   robust-scaler
   sql-transformer
   standard-scaler
   stop-words-remover
   string-indexer
   tokeniser
   tokenizer
   vector-assembler
   vector-indexer
   vector-size-hint
   word-2-vec
   word2vec])

(import-vars
  [zero-one.geni.ml.classification
   decision-tree-classifier
   fm-classifier
   gbt-classifier
   linear-svc
   logistic-regression
   mlp-classifier
   multilayer-perceptron-classifier
   naive-bayes
   one-vs-rest
   random-forest-classifier])

(import-vars
  [zero-one.geni.ml.fpm
   fp-growth
   frequent-pattern-growth
   prefix-span])

(import-vars
  [zero-one.geni.ml.regression
   aft-survival-regression
   decision-tree-regressor
   fm-regressor
   gbt-regressor
   generalised-linear-regression
   generalized-linear-regression
   glm
   isotonic-regression
   linear-regression
   random-forest-regressor])

(import-vars
  [zero-one.geni.ml.recommendation
   als
   alternating-least-squares
   item-factors
   recommend-for-all-items
   recommend-for-all-users
   recommend-for-item-subset
   recommend-for-user-subset
   recommend-items
   recommend-users
   user-factors])

(import-vars
  [zero-one.geni.ml.tuning
   cross-validator
   param-grid
   param-grid-builder
   train-validation-split])

(defn vector-to-array
  ([expr] (vector-to-array (column/->column expr) "float64"))
  ([expr dtype] (functions/vector_to_array (column/->column expr) dtype)))

(def corr polymorphic/corr)

(defn chi-square-test [dataframe features-col label-col]
  (ChiSquareTest/test dataframe (name features-col) (name label-col)))

(defn kolmogorov-smirnov-test [dataframe sample-col dist-name params]
  (KolmogorovSmirnovTest/test dataframe (name sample-col) dist-name (interop/->scala-seq params)))

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

(defn approx-nearest-neighbors
  ([dataset model key-v n-nearest]
   (.approxNearestNeighbors model dataset (interop/dense key-v) n-nearest))
  ([dataset model key-v n-nearest dist-col]
   (.approxNearestNeighbors model dataset (interop/dense key-v) n-nearest dist-col)))
(defn approx-similarity-join
  ([dataset-a dataset-b model threshold]
   (.approxSimilarityJoin model dataset-a dataset-b threshold))
  ([dataset-a dataset-b model threshold dist-col]
   (.approxSimilarityJoin model dataset-a dataset-b threshold dist-col)))
(defn association-rules [model] (.associationRules model))
(defn binary-summary [model] (.binarySummary model))
(defn best-model [model] (.bestModel model))
(defn boundaries [model] (interop/->clojure (.boundaries model)))
(defn category-maps [model] (->> model .categoryMaps interop/scala-map->map))
(defn category-sizes [model] (seq (.categorySizes model)))
(defn cluster-centers [model] (->> model .clusterCenters seq (map interop/->clojure)))
(defn coefficient-matrix [model] (interop/matrix->seqs (.coefficientMatrix model)))
(defn coefficients [model] (interop/vector->seq (.coefficients model)))
(defn depth [model] (.depth model))
(def describe-topics (memfn describeTopics))
(defn estimated-doc-concentration [model] (interop/->clojure (.estimatedDocConcentration model)))
(defn feature-importances [model] (interop/->clojure (.featureImportances model)))
(defn find-frequent-sequential-patterns [dataset prefix-span]
  (.findFrequentSequentialPatterns prefix-span dataset))
(defn freq-itemsets [model] (.freqItemsets model))
(defn gaussians-df [model] (.gaussiansDF model))
(defn get-features-col [model] (.getFeaturesCol model))
(defn get-input-col [model] (.getInputCol model))
(defn get-input-cols [model] (seq (.getInputCols model)))
(defn get-label-col [model] (.getLabelCol model))
(defn get-output-col [model] (.getOutputCol model))
(defn get-output-cols [model] (seq (.getOutputCols model)))
(defn get-prediction-col [model] (.getPredictionCol model))
(defn get-probability-col [model] (.getProbabilityCol model))
(defn get-raw-prediction-col [model] (.getRawPredictionCol model))
(defn get-thresholds [model] (seq (.getThresholds model)))
(defn get-num-trees [model] (.getNumTrees model))
(defn get-size [model] (.getSize model))
(defn idf-vector [model] (interop/vector->seq (.idf model)))
(defn intercept [model] (.intercept model))
(defn intercept-vector [model] (interop/vector->seq (.interceptVector model)))
(defn is-distributed [model] (.isDistributed model))
(defn labels [model] (seq (.labels model)))
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
(defn pi [model] (interop/vector->seq (.pi model)))
(defn root-node [model] (.rootNode model))
(defn scale [model] (.scale model))
(defn summary [model] (.summary model))
(defn supported-optimizers [model] (seq (.supportedOptimizers model)))
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

(defn write-stage!
  ([stage path] (write-stage! stage path {}))
  ([stage path options]
   (let [unconfigured-writer (-> stage
                                 .write
                                 (cond-> (= (:mode options) "overwrite")
                                   .overwrite))
         configured-writer    (reduce
                                (fn [w [k v]] (.option w (name k) v))
                                unconfigured-writer
                                (dissoc options :mode))]
     (.save configured-writer path))))

(defn- load-method? [^java.lang.reflect.Method method]
  (and ; (= 1 (alength ^"[Ljava.lang.Class;" (.getParameterTypes method)))
       (= "load" (.getName method))))

(defn- load-method [cls]
  (->> cls
       .getMethods
       (filter load-method?)
       first))

(defn read-stage! [model-cls path]
  (.invoke (load-method model-cls) model-cls (into-array [path])))

;; Docs
(docs/alter-docs-in-ns!
  'zero-one.geni.ml
  (-> docs/spark-docs :methods :ml :models vals))

(docs/alter-docs-in-ns!
  'zero-one.geni.ml
  (-> docs/spark-docs :methods :ml :features vals))

(docs/alter-docs-in-ns!
  'zero-one.geni.ml
  [(-> docs/spark-docs :classes :ml :stat)])

(docs/add-doc!
  (var idf-vector)
  (-> docs/spark-docs :methods :ml :features :idf :idf))

(docs/add-doc!
  (var pipeline)
  (-> docs/spark-docs :classes :ml :pipeline :pipeline))

(docs/add-doc!
  (var vector-to-array)
  (-> docs/spark-docs :methods :ml :functions :vector-to-array))

;; Aliases
(import-fn approx-nearest-neighbors approx-nearest-neighbours)
(import-fn find-frequent-sequential-patterns find-patterns)
(import-fn freq-itemsets frequent-item-sets)
(import-fn get-features-col features-col)
(import-fn get-input-col input-col)
(import-fn get-input-cols input-cols)
(import-fn get-label-col label-col)
(import-fn get-output-col output-col)
(import-fn get-output-cols output-cols)
(import-fn get-prediction-col prediction-col)
(import-fn get-probability-col probability-col)
(import-fn get-raw-prediction-col raw-prediction-col)
(import-fn get-thresholds thresholds)
(import-fn is-distributed distributed?)
(import-fn pc principal-components)
(import-fn supported-optimizers supported-optimisers)
(import-fn vector-to-array vector->array)

(comment

  (require '[zero-one.geni.docs :as docs])
  (docs/invalid-doc-vars *ns*)

  (count (docs/docless-vars *ns*))
  (-> docs/spark-docs :classes :ml :stat sort)

  (import '(org.apache.spark.ml.classification GBTRegressor))
  (params (GBTRegressor))

  true)

