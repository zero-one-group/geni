(ns zero-one.geni.ml.feature
  (:require
   [zero-one.geni.utils :refer [class-named import-fn]]
   [zero-one.geni.docs :as docs]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.spark :as spark])
  (:import
   (org.apache.spark.ml.feature CountVectorizerModel
                                StopWordsRemover
                                StringIndexerModel)))

(interop/def-stages org.apache.spark.ml.feature
  [binarizer Binarizer]
  [bucketed-random-projection-lsh BucketedRandomProjectionLSH]
  [bucketizer Bucketizer]
  [chi-sq-selector ChiSqSelector]
  [count-vectorizer CountVectorizer]
  [dct DCT]
  [elementwise-product ElementwiseProduct]
  [feature-hasher FeatureHasher]
  [hashing-tf HashingTF]
  [idf IDF]
  [imputer Imputer]
  [index-to-string IndexToString]
  [interaction Interaction]
  [max-abs-scaler MaxAbsScaler]
  [min-hash-lsh MinHashLSH]
  [min-max-scaler MinMaxScaler]
  [n-gram NGram]
  [normalizer Normalizer]
  [one-hot-encoder OneHotEncoder]
  [pca PCA]
  [polynomial-expansion PolynomialExpansion]
  [quantile-discretizer QuantileDiscretizer]
  [regex-tokenizer RegexTokenizer]
  [robust-scaler RobustScaler]
  [sql-transformer SQLTransformer]
  [standard-scaler StandardScaler]
  [stop-words-remover StopWordsRemover]
  [string-indexer StringIndexer]
  [tokenizer Tokenizer]
  [vector-assembler VectorAssembler]
  [vector-indexer VectorIndexer]
  [vector-size-hint VectorSizeHint]
  [word-2-vec Word2Vec]
  [r-formula RFormula
   "An estimator that turns an R model formula, such as `\"y ~ a + b\"`, into
  a features vector column and a label column, `:features-col` and
  `:label-col`. String columns are one-hot encoded, and a string label is
  indexed. It takes `:formula` and Spark's other RFormula params.

  ```clojure
  (ml/r-formula {:formula \"Price ~ Rooms + Type\"})
  ```"]
  [univariate-feature-selector UnivariateFeatureSelector
   "An estimator that selects the features that best predict the label, each
  scored on its own: by an F-test, ANOVA or chi-squared, as `:feature-type`
  and `:label-type`, \"categorical\" or \"continuous\", say. `:selection-mode`
  and `:selection-threshold` say how many to keep. The fitted model's
  `selected-features` are their indices.

  ```clojure
  (ml/univariate-feature-selector {:feature-type \"continuous\"
                                   :label-type \"categorical\"
                                   :selection-threshold 2})
  ```"]
  [variance-threshold-selector VarianceThresholdSelector
   "An estimator that drops the features whose sample variance is at most
  `:variance-threshold`, 0.0 by default, which drops the constant ones. The
  fitted model's `selected-features` are the indices it keeps."]
  [vector-slicer VectorSlicer
   "A transformer that keeps some of a vector column's features: those at
  `:indices`, and then those named in `:names`, from the column's
  attributes.

  ```clojure
  (ml/vector-slicer {:input-col :features :output-col :sliced :indices [0 2]})
  ```"])

(defn target-encoder
  "An estimator that encodes categorical columns, `:input-cols`, as their
  mean label, smoothed towards the overall mean by `:smoothing`, with
  `:target-type` \"binary\" or \"continuous\". It needs Spark 4.0."
  [params]
  (spark/require-version! [4 0] "ml/target-encoder")
  (interop/instantiate (class-named "org.apache.spark.ml.feature.TargetEncoder") params))

(defn- ->strings
  "Labels or terms as a String array: keywords by name, and anything else by
  `str`, as a number label's string."
  ^"[Ljava.lang.String;" [values]
  (into-array String (map #(if (keyword? %) (name %) (str %)) values)))

(defn string-indexer-model
  "A StringIndexer model made from known labels rather than fitted: index 0
  for the first label, and so on. It takes `:labels` and `:input-col`, or
  `:labels-array`, one vector of labels per column, and `:input-cols`, and
  the model's other params, such as `:output-col` and `:handle-invalid`.

  ```clojure
  (ml/string-indexer-model {:labels [\"low\" \"high\"] :input-col :level :output-col :i})
  ```"
  [{:keys [labels labels-array] :as params}]
  (when-not (= 1 (count (filter some? [labels labels-array])))
    (throw (ex-info "string-indexer-model takes either :labels or :labels-array." {})))
  (interop/set-params!
   (if labels
     (StringIndexerModel. (->strings labels))
     (StringIndexerModel. ^"[[Ljava.lang.String;"
      (into-array (Class/forName "[Ljava.lang.String;")
                  (map ->strings labels-array))))
   (dissoc params :labels :labels-array)))

(defn count-vectorizer-model
  "A CountVectorizer model made from a known vocabulary rather than fitted,
  which counts the terms of `:vocabulary` in an array column. It takes the
  model's params, such as `:input-col`, `:output-col`, `:min-tf` and
  `:binary`.

  ```clojure
  (ml/count-vectorizer-model {:vocabulary [\"a\" \"b\"] :input-col :words :output-col :counts})
  ```"
  [{:keys [vocabulary] :as params}]
  (when-not (seq vocabulary)
    (throw (ex-info "count-vectorizer-model takes a :vocabulary." {})))
  (interop/set-params!
   (CountVectorizerModel. (->strings vocabulary))
   (dissoc params :vocabulary)))

(defn load-default-stop-words
  "Spark's stop words for a language, such as `:english`, `:french` or
  `:german`, for `stop-words-remover`'s `:stop-words`."
  [language]
  (seq (StopWordsRemover/loadDefaultStopWords (name language))))

;; Docs
(docs/alter-docs-in-ns!
 'zero-one.geni.ml.feature
 [(-> docs/spark-docs :classes :ml :feature)])

;; Aliases
(import-fn binarizer binariser)
(import-fn bucketizer bucketiser)
(import-fn count-vectorizer count-vectoriser)
(import-fn dct discrete-cosine-transform)
(import-fn normalizer normaliser)
(import-fn quantile-discretizer quantile-discretiser)
(import-fn regex-tokenizer regex-tokeniser)
(import-fn tokenizer tokeniser)
(import-fn word-2-vec word2vec)

