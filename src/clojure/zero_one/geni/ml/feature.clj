(ns zero-one.geni.ml.feature
  (:require
   [zero-one.geni.utils :refer [class-named import-fn]]
   [zero-one.geni.docs :as docs]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.ml.default-stop-words :refer [default-stop-words]]
   [zero-one.geni.spark :as spark])
  (:import
   (org.apache.spark.ml.feature Binarizer
                                Bucketizer
                                BucketedRandomProjectionLSH
                                ChiSqSelector
                                CountVectorizer
                                CountVectorizerModel
                                DCT
                                ElementwiseProduct
                                FeatureHasher
                                HashingTF
                                IDF
                                Imputer
                                IndexToString
                                Interaction
                                MaxAbsScaler
                                MinHashLSH
                                MinMaxScaler
                                NGram
                                Normalizer
                                OneHotEncoder
                                PCA
                                PolynomialExpansion
                                QuantileDiscretizer
                                RFormula
                                RegexTokenizer
                                RobustScaler
                                SQLTransformer
                                StandardScaler
                                StopWordsRemover
                                StringIndexer
                                StringIndexerModel
                                Tokenizer
                                UnivariateFeatureSelector
                                VarianceThresholdSelector
                                VectorAssembler
                                VectorIndexer
                                VectorSizeHint
                                VectorSlicer
                                Word2Vec)))

(defn stop-words-remover [params]
  (let [defaults {:locale         "en_US",
                  :stop-words     default-stop-words
                  :case-sensitive false}]
    (interop/instantiate StopWordsRemover defaults params)))

(defn chi-sq-selector [params]
  (let [defaults {:fdr              0.05,
                  :fpr              0.05,
                  :label-col        "label",
                  :percentile       0.1,
                  :selector-type    "numTopFeatures",
                  :num-top-features 50,
                  :fwe              0.05,
                  :features-col     "features"}]
    (interop/instantiate ChiSqSelector defaults params)))

(defn vector-assembler [params]
  (let [defaults {:handle-invalid "error"}]
    (interop/instantiate VectorAssembler defaults params)))

(defn feature-hasher [params]
  (let [defaults {:num-features 262144}]
    (interop/instantiate FeatureHasher defaults params)))

(defn n-gram [params]
  (let [defaults {:n 2}]
    (interop/instantiate NGram defaults params)))

(defn binarizer [params]
  (let [defaults {:threshold 0.5}]
    (interop/instantiate Binarizer defaults params)))

(defn pca [params]
  (interop/instantiate PCA params))

(defn polynomial-expansion [params]
  (let [defaults {:degree 2}]
    (interop/instantiate PolynomialExpansion defaults params)))

(defn dct [params]
  (let [defaults {:inverse false}]
    (interop/instantiate DCT defaults params)))

(defn string-indexer [params]
  (let [defaults {:handle-invalid "error"
                  :string-order-type "frequencyDesc"}]
    (interop/instantiate StringIndexer defaults params)))

(defn index-to-string [params]
  (interop/instantiate IndexToString params))

(defn one-hot-encoder [params]
  (let [defaults {:drop-last true :handle-invalid "error"}]
    (interop/instantiate OneHotEncoder defaults params)))

(defn vector-indexer [params]
  (let [defaults {:max-categories 20 :handle-invalid "error"}]
    (interop/instantiate VectorIndexer defaults params)))

(defn interaction [params]
  (interop/instantiate Interaction params))

(defn normalizer [params]
  (let [defaults {:p 2.0}]
    (interop/instantiate Normalizer defaults params)))

(defn standard-scaler [params]
  (let [defaults {:with-std true :with-mean false}]
    (interop/instantiate StandardScaler defaults params)))

(defn min-max-scaler [params]
  (let [defaults {:min 0.0 :max 1.0}]
    (interop/instantiate MinMaxScaler defaults params)))

(defn max-abs-scaler [params]
  (interop/instantiate MaxAbsScaler params))

(defn bucketizer [params]
  (let [defaults {:handle-invalid "error"}]
    (interop/instantiate Bucketizer defaults params)))

(defn elementwise-product [params]
  (let [params (if (:scaling-vec params)
                 (update params :scaling-vec interop/dense)
                 params)]
    (interop/instantiate ElementwiseProduct params)))

(defn sql-transformer [params]
  (interop/instantiate SQLTransformer params))

(defn vector-size-hint [params]
  (let [defaults {:handle-invalid "error"}]
    (interop/instantiate VectorSizeHint defaults params)))

(defn quantile-discretizer [params]
  (let [defaults {:handle-invalid "error"
                  :num-buckets    2
                  :relative-error 0.001}]
    (interop/instantiate QuantileDiscretizer defaults params)))

(defn imputer [params]
  (let [defaults {:missing-value ##NaN
                  :strategy      "mean"}]
    (interop/instantiate Imputer defaults params)))

(defn bucketed-random-projection-lsh [params]
  (let [defaults {:num-hash-tables 1
                  :seed            772209414}]
    (interop/instantiate BucketedRandomProjectionLSH defaults params)))

(defn min-hash-lsh [params]
  (let [defaults {:num-hash-tables 1
                  :seed            772209414}]
    (interop/instantiate MinHashLSH defaults params)))

(defn count-vectorizer [params]
  (let [defaults {:vocab-size 262144,
                  :min-df     1.0,
                  :min-tf     1.0,
                  :binary     false,
                  :max-df     9.223372036854776E18}]
    (interop/instantiate CountVectorizer defaults params)))

(defn idf [params]
  (let [defaults {:min-doc-freq 0}]
    (interop/instantiate IDF defaults params)))

(defn tokenizer [params]
  (interop/instantiate Tokenizer params))

(defn hashing-tf [params]
  (let [defaults {:binary       false
                  :num-features 262144}]
    (interop/instantiate HashingTF defaults params)))

(defn word-2-vec [params]
  (let [defaults {:max-iter            1,
                  :step-size           0.025,
                  :window-size         5,
                  :max-sentence-length 1000,
                  :num-partitions      1,
                  :seed                -1961189076,
                  :vector-size         100,
                  :min-count           5}]
    (interop/instantiate Word2Vec defaults params)))

(defn regex-tokenizer [params]
  (let [defaults {:to-lowercase true,
                  :pattern "\\s+",
                  :min-token-length 1,
                  :gaps true}]
    (interop/instantiate RegexTokenizer defaults params)))

(defn r-formula
  "An estimator that turns an R model formula, such as `\"y ~ a + b\"`, into
  a features vector column and a label column, `:features-col` and
  `:label-col`. String columns are one-hot encoded, and a string label is
  indexed. It takes `:formula` and Spark's other RFormula params.

  ```clojure
  (ml/r-formula {:formula \"Price ~ Rooms + Type\"})
  ```"
  [params]
  (interop/instantiate RFormula params))

(defn univariate-feature-selector
  "An estimator that selects the features that best predict the label, each
  scored on its own: by an F-test, ANOVA or chi-squared, as `:feature-type`
  and `:label-type`, \"categorical\" or \"continuous\", say. `:selection-mode`
  and `:selection-threshold` say how many to keep. The fitted model's
  `selected-features` are their indices.

  ```clojure
  (ml/univariate-feature-selector {:feature-type \"continuous\"
                                   :label-type \"categorical\"
                                   :selection-threshold 2})
  ```"
  [params]
  (interop/instantiate UnivariateFeatureSelector params))

(defn variance-threshold-selector
  "An estimator that drops the features whose sample variance is at most
  `:variance-threshold`, 0.0 by default, which drops the constant ones. The
  fitted model's `selected-features` are the indices it keeps."
  [params]
  (interop/instantiate VarianceThresholdSelector params))

(defn vector-slicer
  "A transformer that keeps some of a vector column's features: those at
  `:indices`, and then those named in `:names`, from the column's
  attributes.

  ```clojure
  (ml/vector-slicer {:input-col :features :output-col :sliced :indices [0 2]})
  ```"
  [params]
  (interop/instantiate VectorSlicer params))

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

(defn robust-scaler [params]
  (let [defaults {:upper          0.75,
                  :relative-error 0.001,
                  :with-centering false,
                  :lower          0.25,
                  :with-scaling   true}]
    (interop/instantiate RobustScaler defaults params)))

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

