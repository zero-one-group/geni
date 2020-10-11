(ns zero-one.geni.ml.feature
  (:require
    [potemkin :refer [import-fn]]
    [zero-one.geni.docs :as docs]
    [zero-one.geni.interop :as interop]
    [zero-one.geni.ml.default-stop-words :refer [default-stop-words]])
  (:import
    (org.apache.spark.ml.feature Binarizer
                                 Bucketizer
                                 BucketedRandomProjectionLSH
                                 ChiSqSelector
                                 CountVectorizer
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
                                 RegexTokenizer
                                 RobustScaler
                                 SQLTransformer
                                 StandardScaler
                                 StopWordsRemover
                                 StringIndexer
                                 Tokenizer
                                 VectorAssembler
                                 VectorIndexer
                                 VectorSizeHint
                                 Word2Vec)))

(defn stop-words-remover [params]
  (let [defaults {:locale         "en_US",
                  :stop-words     default-stop-words
                  :case-sensitive false}
        props    (merge defaults params)]
    (interop/instantiate StopWordsRemover props)))

(defn chi-sq-selector [params]
  (let [defaults {:fdr              0.05,
                  :fpr              0.05,
                  :label-col        "label",
                  :percentile       0.1,
                  :selector-type    "numTopFeatures",
                  :num-top-features 50,
                  :fwe              0.05,
                  :features-col     "features"}
        props    (merge defaults params)]
    (interop/instantiate ChiSqSelector props)))

(defn vector-assembler [params]
  (let [defaults {:handle-invalid "error"}
        props    (merge defaults params)]
    (interop/instantiate VectorAssembler props)))

(defn feature-hasher [params]
  (let [defaults {:num-features 262144}
        props    (merge defaults params)]
    (interop/instantiate FeatureHasher props)))

(defn n-gram [params]
  (let [defaults {:n 2}
        props    (merge defaults params)]
    (interop/instantiate NGram props)))

(defn binarizer [params]
  (let [defaults {:threshold 0.5}
        props    (merge defaults params)]
    (interop/instantiate Binarizer props)))

(defn pca [params]
  (interop/instantiate PCA params))

(defn polynomial-expansion [params]
  (let [defaults {:degree 2}
        props    (merge defaults params)]
    (interop/instantiate PolynomialExpansion props)))

(defn dct [params]
  (let [defaults {:inverse false}
        props    (merge defaults params)]
    (interop/instantiate DCT props)))

(defn string-indexer [params]
  (let [defaults {:handle-invalid "error"
                  :string-order-type "frequencyDesc"}
        props    (merge defaults params)]
    (interop/instantiate StringIndexer props)))

(defn index-to-string [params]
  (interop/instantiate IndexToString params))

(defn one-hot-encoder [params]
  (let [defaults {:drop-last true :handle-invalid "error"}
        props    (merge defaults params)]
    (interop/instantiate OneHotEncoder props)))

(defn vector-indexer [params]
  (let [defaults {:max-categories 20 :handle-invalid "error"}
        props    (merge defaults params)]
    (interop/instantiate VectorIndexer props)))

(defn interaction [params]
  (interop/instantiate Interaction params))

(defn normalizer [params]
  (let [defaults {:p 2.0}
        props    (merge defaults params)]
    (interop/instantiate Normalizer props)))

(defn standard-scaler [params]
  (let [defaults {:with-std true :with-mean false}
        props    (merge defaults params)]
    (interop/instantiate StandardScaler props)))

(defn min-max-scaler [params]
  (let [defaults {:min 0.0 :max 1.0}
        props    (merge defaults params)]
    (interop/instantiate MinMaxScaler props)))

(defn max-abs-scaler [params]
  (interop/instantiate MaxAbsScaler params))

(defn bucketizer [params]
  (let [defaults {:handle-invalid "error"}
        props    (merge defaults params)]
    (interop/instantiate Bucketizer props)))

(defn elementwise-product [params]
  (let [params (if (:scaling-vec params)
                 (update params :scaling-vec interop/dense)
                 params)]
    (interop/instantiate ElementwiseProduct params)))

(defn sql-transformer [params]
  (interop/instantiate SQLTransformer params))

(defn vector-size-hint [params]
  (let [defaults {:handle-invalid "error"}
        props    (merge defaults params)]
    (interop/instantiate VectorSizeHint props)))

(defn quantile-discretizer [params]
  (let [defaults {:handle-invalid "error"
                  :num-buckets    2
                  :relative-error 0.001}
        props    (merge defaults params)]
    (interop/instantiate QuantileDiscretizer props)))

(defn imputer [params]
  (let [defaults {:missing-value ##NaN
                  :strategy      "mean"}
        props    (merge defaults params)]
    (interop/instantiate Imputer props)))

(defn bucketed-random-projection-lsh [params]
  (let [defaults {:num-hash-tables 1
                  :seed            772209414}
        props    (merge defaults params)]
    (interop/instantiate BucketedRandomProjectionLSH props)))

(defn min-hash-lsh [params]
  (let [defaults {:num-hash-tables 1
                  :seed            772209414}
        props    (merge defaults params)]
    (interop/instantiate MinHashLSH props)))

(defn count-vectorizer [params]
  (let [defaults {:vocab-size 262144,
                  :min-df     1.0,
                  :min-tf     1.0,
                  :binary     false,
                  :max-df     9.223372036854776E18}
        props    (merge defaults params)]
    (interop/instantiate CountVectorizer props)))

(defn idf [params]
  (let [defaults {:min-doc-freq 0}
        props    (merge defaults params)]
    (interop/instantiate IDF props)))

(defn tokenizer [params]
  (interop/instantiate Tokenizer params))

(defn hashing-tf [params]
  (let [defaults {:binary       false
                  :num-features 262144}
        props    (merge defaults params)]
    (interop/instantiate HashingTF props)))


(defn word-2-vec [params]
  (let [defaults {:max-iter            1,
                  :step-size           0.025,
                  :window-size         5,
                  :max-sentence-length 1000,
                  :num-partitions      1,
                  :seed                -1961189076,
                  :vector-size         100,
                  :min-count           5}
        props    (merge defaults params)]
    (interop/instantiate Word2Vec props)))

(defn regex-tokenizer [params]
  (let [defaults {:to-lowercase true,
                  :pattern "\\s+",
                  :min-token-length 1,
                  :gaps true}
        props    (merge defaults params)]
    (interop/instantiate RegexTokenizer props)))

(defn robust-scaler [params]
  (let [defaults {:upper          0.75,
                  :relative-error 0.001,
                  :with-centering false,
                  :lower          0.25,
                  :with-scaling   true}
        props    (merge defaults params)]
    (interop/instantiate RobustScaler props)))

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

