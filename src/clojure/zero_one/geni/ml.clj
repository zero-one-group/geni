(ns zero-one.geni.ml
  (:refer-clojure :exclude [range])
  (:require
   [clojure.walk :refer [keywordize-keys]]
   [zero-one.geni.utils :refer [->camel-case ->kebab-case class-named import-fn import-vars]]
   [zero-one.geni.core.column :as column]
   [zero-one.geni.core.polymorphic :as polymorphic]
   [zero-one.geni.defaults :as defaults]
   [zero-one.geni.docs :as docs]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.ml.classification]
   [zero-one.geni.ml.clustering]
   [zero-one.geni.ml.evaluation]
   [zero-one.geni.ml.feature]
   [zero-one.geni.ml.fpm]
   [zero-one.geni.ml.recommendation]
   [zero-one.geni.ml.regression]
   [zero-one.geni.ml.summary]
   [zero-one.geni.ml.tuning]
   [zero-one.geni.ml.xgb])
  (:import
   (org.apache.spark.ml Pipeline
                        PipelineStage
                        functions)
   (org.apache.spark.ml.param Params)
   (org.apache.spark.ml.stat ChiSquareTest
                             Correlation
                             KolmogorovSmirnovTest
                             Summarizer)))

(import-vars
 [zero-one.geni.ml.xgb
  write-native-model!
  xgboost-classifier
  xgboost-ranker
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
  count-vectorizer-model
  dct
  discrete-cosine-transform
  elementwise-product
  feature-hasher
  hashing-tf
  idf
  imputer
  index-to-string
  interaction
  load-default-stop-words
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
  r-formula
  regex-tokeniser
  regex-tokenizer
  robust-scaler
  sql-transformer
  standard-scaler
  stop-words-remover
  string-indexer
  string-indexer-model
  target-encoder
  tokeniser
  tokenizer
  univariate-feature-selector
  variance-threshold-selector
  vector-assembler
  vector-indexer
  vector-size-hint
  vector-slicer
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
  avg-metrics
  cross-validator
  param-grid
  param-grid-builder
  sub-models
  train-validation-split
  validation-metrics])

(import-vars
 [zero-one.geni.ml.summary
  binary-summary
  summary])

(defn vector-to-array
  ([expr] (vector-to-array (column/->column expr) "float64"))
  ([expr dtype] (functions/vector_to_array (column/->column expr) dtype)))

(defn array-to-vector
  "A column of MLlib dense vectors from a column of arrays of numbers, as a
  model's features column takes them. `vector-to-array` goes the other way."
  [expr]
  (functions/array_to_vector (column/->column expr)))

(defn chi-square-test
  "Pearson's chi-squared test of independence of each feature in
  `features-col`, a vector column, against `label-col`: a DataFrame of one
  row with the p-values, the degrees of freedom and the statistics, as
  vectors. With `flatten` true, a row per feature instead."
  ([dataframe features-col label-col]
   (ChiSquareTest/test dataframe (name features-col) (name label-col)))
  ([dataframe features-col label-col flatten]
   (ChiSquareTest/test dataframe (name features-col) (name label-col) (boolean flatten))))

(defn summarizer
  "An aggregate column of a vector column's statistics, as a struct with a
  field per metric: `:mean`, `:sum`, `:variance`, `:std`, `:count`,
  `:num-non-zeros`, `:max`, `:min`, `:norm-l2` and `:norm-l1`, each a vector
  but `:count`. The fields take Spark's names, such as `numNonZeros`. With
  `weight-col`, the metrics are weighted by it.

  ```clojure
  (g/agg dataframe {:stats (ml/summarizer :features [:mean :variance])})
  ```"
  ([features-col metrics]
   (.summary (Summarizer/metrics ^"[Ljava.lang.String;" (into-array String (map ->camel-case metrics)))
             (column/->column features-col)))
  ([features-col metrics weight-col]
   (.summary (Summarizer/metrics ^"[Ljava.lang.String;" (into-array String (map ->camel-case metrics)))
             (column/->column features-col)
             (column/->column weight-col))))

(defn kolmogorov-smirnov-test [dataframe sample-col dist-name params]
  (KolmogorovSmirnovTest/test dataframe (name sample-col) dist-name (interop/->scala-seq params)))

(defn pipeline [& stages]
  (-> (Pipeline.)
      (.setStages (into-array PipelineStage stages))))

(defn stage
  "A Spark ML stage of any class, with `params` set through its setters as
  Geni's own stages have them: `{:input-cols [:document] :output-col :token}`
  calls `setInputCols` and `setOutputCol`. This covers stages that Geni has no
  function for, such as Spark NLP's annotators. `cls` is a class, a class name,
  or a stage made already, such as a pretrained model, whose params are set in
  place. A param that the class has no setter for throws, naming the closest.

  ```clojure
  (ml/stage \"com.johnsnowlabs.nlp.DocumentAssembler\"
            {:input-col :text :output-col :document})
  (ml/stage (LemmatizerModel/pretrained) {:input-cols [:token] :output-col :lemma})
  ```"
  ([cls] (stage cls {}))
  ([cls params]
   (cond
     (class? cls)              (interop/instantiate cls params)
     (or (string? cls)
         (symbol? cls))        (if-let [found (class-named (str cls))]
                                 (interop/instantiate found params)
                                 (throw (ex-info (str "There's no class " cls " on the classpath."
                                                      " Add the library that has it to your deps.")
                                                 {:class-name (str cls)})))
     (instance? Params cls)    (interop/set-params! cls params)
     :else                     (throw (IllegalArgumentException.
                                       (str "ml/stage takes a class, a class name or a stage, not "
                                            (pr-str cls) "."))))))

(defn fit [dataframe estimator]
  (.fit estimator dataframe))

(defn transform [dataframe transformer]
  (.transform transformer dataframe))

(defn evaluate
  "The metric that `evaluator` gives `dataframe`'s predictions, such as the
  area under the ROC curve. Given a model in place of an evaluator, a model
  that has `evaluate`, such as a logistic or linear regression's, it's the
  model's summary of how it does on `dataframe`, as `summary` gives for the
  training data."
  [dataframe evaluator]
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

(defn assign-clusters
  "Power iteration clustering's clusters for the vertices of a graph, whose
  edges are the rows of `dataset`, with the columns that the
  `power-iteration-clustering` names, `src`, `dst` and an optional weight: a
  DataFrame of `id` and `cluster`.

  ```clojure
  (ml/assign-clusters edges (ml/power-iteration-clustering {:k 2}))
  ```"
  [dataset power-iteration-clustering]
  (.assignClusters power-iteration-clustering dataset))

(defn larger-better?
  "Whether a larger metric is a better one, for an evaluator as it's set up,
  as for `avg-metrics`: true for the area under the ROC curve, and false for
  RMSE."
  [evaluator]
  (.isLargerBetter evaluator))

(defn correlation
  "The correlation matrix of a vector column, by `method`, \"pearson\" by
  default or \"spearman\": a DataFrame of one row, whose one value is the
  matrix."
  ([dataframe col] (correlation dataframe col "pearson"))
  ([dataframe col method]
   (Correlation/corr dataframe (name col) (name method))))
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

;; Predictions for one row, and more of the models' attributes

(defn- ->features
  "One row's features, for a model's single prediction: a collection of
  numbers as a dense vector, a sparse vector's map, as `g/collect` gives it,
  as a sparse vector, and a number, which isotonic regression takes, as a
  double."
  [features]
  (cond
    (number? features) (double features)
    (map? features)    (let [{:keys [size indices values]} features]
                         (interop/->sparse-vector size indices values))
    (coll? features)   (interop/->dense-vector features)
    :else              features))

(defn predict
  "The model's prediction for one row's features, a vector of numbers, an
  MLlib vector or a sparse vector's map, as `g/collect` gives one, as its
  `transform` makes it: a label for a classifier, a
  value for a regressor, and a cluster for a clustering model. Isotonic
  regression takes one number.

  ```clojure
  (ml/predict model [0.5 1.0 2.0])
  ```"
  [model features]
  (.predict model (->features features)))

(defn predict-raw
  "A classifier's raw prediction for one row's features, such as the margin
  of each class for logistic regression."
  [model features]
  (interop/vector->seq (.predictRaw model (->features features))))

(defn predict-probability
  "A probabilistic classifier's, or a Gaussian mixture's, probability of each
  class, or cluster, for one row's features."
  [model features]
  (interop/vector->seq (.predictProbability model (->features features))))

(defn predict-leaf
  "The leaf that one row's features end in: its index for a decision tree,
  and one index per tree for a forest or boosted trees."
  [model features]
  (let [leaf (.predictLeaf model (->features features))]
    (if (number? leaf) leaf (interop/vector->seq leaf))))

(defn predict-quantiles
  "An AFT survival regression's quantiles for one row's features, at the
  model's `:quantile-probabilities`."
  [model features]
  (interop/vector->seq (.predictQuantiles model (->features features))))

(defn to-debug-string
  "A tree model's trees as text, with each node's split and prediction."
  [model]
  (.toDebugString model))

(defn evaluate-each-iteration
  "A gradient-boosted trees model's loss on `dataset` after each iteration:
  for a regressor, by the loss given, \"squared\" or \"absolute\"."
  ([dataset model] (seq (.evaluateEachIteration model dataset)))
  ([dataset model loss] (seq (.evaluateEachIteration model dataset (name loss)))))

(defn explained-variance
  "The share of the variance that each of a PCA model's components explains."
  [model]
  (interop/vector->seq (.explainedVariance model)))

(defn doc-freq
  "The number of documents that each term occurs in, by index, for an IDF
  model."
  [model]
  (seq (.docFreq model)))

(defn num-docs
  "The number of documents that an IDF model was fitted on."
  [model]
  (.numDocs model))

(defn find-synonyms
  "The `n` words closest to a word, or to a vector, of numbers or an MLlib
  vector, in a Word2Vec model, as a DataFrame of `word` and `similarity`."
  [model word-or-vector n]
  (.findSynonyms model
                 (cond
                   (coll? word-or-vector)                     (interop/->dense-vector word-or-vector)
                   (or (string? word-or-vector)
                       (keyword? word-or-vector)
                       (symbol? word-or-vector))              (name word-or-vector)
                   :else                                      word-or-vector)
                 (int n)))

(defn get-vectors
  "A Word2Vec model's words and their vectors, as a DataFrame of `word` and
  `vector`."
  [model]
  (.getVectors model))

(defn topics-matrix
  "An LDA model's topics: a row per term, a column per topic. A distributed
  model gathers it to the driver."
  [model]
  (interop/matrix->seqs (.topicsMatrix model)))

(defn log-prior
  "A distributed LDA model's log prior of its parameters, given its
  hyperparameters."
  [model]
  (.logPrior model))

(defn training-log-likelihood
  "A distributed LDA model's log likelihood of the documents it was fitted
  on."
  [model]
  (.trainingLogLikelihood model))

(defn to-local
  "A distributed LDA model as a local one, without the training data."
  [model]
  (.toLocal model))

(defn get-checkpoint-files
  "The checkpoint files that a distributed LDA model keeps, for its
  `:keep-last-checkpoint` param."
  [model]
  (seq (.getCheckpointFiles model)))

(defn median
  "The median of each feature, for a RobustScaler model."
  [model]
  (interop/vector->seq (.median model)))

(defn range
  "The quantile range of each feature, for a RobustScaler model."
  [model]
  (interop/vector->seq (.range model)))

(defn sigma
  "A Gaussian naive Bayes model's variances, a row per class and a column per
  feature."
  [model]
  (interop/matrix->seqs (.sigma model)))

(defn factors
  "A factorisation machine's factors, a row per feature."
  [model]
  (interop/matrix->seqs (.factors model)))

(defn linear
  "A factorisation machine's linear terms, one per feature."
  [model]
  (interop/vector->seq (.linear model)))

(defn compute-cost
  "A bisecting k-means model's sum of squared distances from the rows of
  `dataset` to their nearest centre."
  [dataset model]
  (.computeCost model dataset))

(defn rank
  "The rank of an ALS model's factors."
  [model]
  (.rank model))

(defn get-splits
  "A Bucketizer's splits."
  [model]
  (seq (.getSplits model)))

(defn get-splits-array
  "A Bucketizer's splits for each of its `:input-cols`."
  [model]
  (map seq (.getSplitsArray model)))

(defn labels-array
  "A StringIndexer model's labels for each of its input columns, in the
  order of their indices."
  [model]
  (map seq (.labelsArray model)))

(defn has-summary
  "Whether a model has a training summary, which a model loaded from disk
  doesn't."
  [model]
  (.hasSummary model))

(defn selected-features
  "The indices of the features that a ChiSqSelector,
  UnivariateFeatureSelector or VarianceThresholdSelector model keeps."
  [model]
  (seq (.selectedFeatures model)))

(defn resolved-formula-string
  "An RFormula model's formula, with its terms resolved against the columns
  it was fitted on."
  [model]
  (str (.resolvedFormula model)))

(defn write-stage!
  "Save a PipelineStage to the specified path, with Geni's default session."
  ([stage path] (write-stage! stage path {}))
  ([stage path options]
   (let [unconfigured-writer (-> stage
                                 .write
                                 (.session @defaults/spark)
                                 (cond-> (= "overwrite" (some-> (:mode options) name))
                                   .overwrite))
         configured-writer    (reduce
                               (fn [w [k v]] (.option w (name k) v))
                               unconfigured-writer
                               (dissoc options :mode))]
     (.save configured-writer path))))

(defn- read-method [^Class cls]
  (->> (.getMethods cls)
       (filter #(and (= "read" (.getName ^java.lang.reflect.Method %))
                     (zero? (.getParameterCount ^java.lang.reflect.Method %))))
       first))

(defn read-stage!
  "Load a saved PipelineStage, with Geni's default session."
  [model-cls path]
  (-> (.invoke ^java.lang.reflect.Method (read-method model-cls) model-cls (object-array 0))
      (.session @defaults/spark)
      (.load path)))

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
(import-fn array-to-vector array->vector)
(import-fn has-summary has-summary?)
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
(import-fn polymorphic/corr corr)
(import-fn supported-optimizers supported-optimisers)
(import-fn vector-to-array vector->array)
