(ns zero-one.geni.ml-feature
  (:require
    [zero-one.geni.interop :as interop]
    [zero-one.geni.default-stop-words :refer [default-stop-words]])
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
                                 OneHotEncoderEstimator
                                 PCA
                                 PolynomialExpansion
                                 QuantileDiscretizer
                                 RegexTokenizer
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
  (let [defaults {:output-col "stopWords_9a3d7440ac5c__output",
                    :locale "en_US",
                    :stop-words default-stop-words
                    :case-sensitive false}
        props    (merge defaults params)]
    (interop/instantiate StopWordsRemover props)))

(defn chi-sq-selector [params]
  (let [defaults {:fdr 0.05,
                  :fpr 0.05,
                  :label-col "label",
                  :percentile 0.1,
                  :selector-type "numTopFeatures",
                  :num-top-features 50,
                  :fwe 0.05,
                  :features-col "features"}
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

(defn binariser [params]
  (let [defaults {:threshold 0.5}
        props    (merge defaults params)]
    (interop/instantiate Binarizer props)))
(def binarizer binariser)

(defn pca [params]
  (interop/instantiate PCA params))

(defn polynomial-expansion [params]
  (let [defaults {:degree 2}
        props    (merge defaults params)]
    (interop/instantiate PolynomialExpansion props)))

(defn discrete-cosine-transform [params]
  (let [defaults {:inverse false}
        props    (merge defaults params)]
    (interop/instantiate DCT props)))
(def dct discrete-cosine-transform)

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
    (interop/instantiate OneHotEncoderEstimator props)))
(def one-hot-encoder-estimator one-hot-encoder)

(defn vector-indexer [params]
  (let [defaults {:max-categories 20 :handle-invalid "error"}
        props    (merge defaults params)]
    (interop/instantiate VectorIndexer props)))

(defn interaction [params]
  (interop/instantiate Interaction params))

(defn normaliser [params]
  (let [defaults {:p 2.0}
        props    (merge defaults params)]
    (interop/instantiate Normalizer props)))
(def normalizer normaliser)

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

(defn bucketiser [params]
  (let [defaults {:handle-invalid "error"}
        props    (merge defaults params)]
    (interop/instantiate Bucketizer props)))
(def bucketizer bucketiser)

(defn elementwise-product [params]
  (let [params (if (:scaling-vec params)
                 (update params :scaling-vec interop/->scala-coll)
                 params)]
    (interop/instantiate ElementwiseProduct params)))

(defn sql-transformer [params]
  (interop/instantiate SQLTransformer params))

(defn vector-size-hint [params]
  (let [defaults {:handle-invalid "error"}
        props    (merge defaults params)]
    (interop/instantiate VectorSizeHint props)))

(defn quantile-discretiser [params]
  (let [defaults {:handle-invalid "error"
                  :num-buckets    2
                  :relative-error 0.001}
        props    (merge defaults params)]
    (interop/instantiate QuantileDiscretizer props)))
(def quantile-discretizer quantile-discretiser)

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

(defn count-vectoriser [params]
  (let [defaults {:vocab-size 262144,
                  :min-df     1.0,
                  :min-tf     1.0,
                  :binary     false,
                  :max-df     9.223372036854776E18}
        props    (merge defaults params)]
    (interop/instantiate CountVectorizer props)))
(def count-vectorizer count-vectoriser)

(defn idf [params]
  (let [defaults {:min-doc-freq 0}
        props    (merge defaults params)]
    (interop/instantiate IDF props)))

(defn tokeniser [params]
  (interop/instantiate Tokenizer params))
(def tokenizer tokeniser)

(defn hashing-tf [params]
  (let [defaults {:binary       false
                  :num-features 262144}
        props    (merge defaults params)]
    (interop/instantiate HashingTF props)))


(defn word2vec [params]
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

(defn regex-tokeniser [params]
  (let [defaults {:to-lowercase true,
                  :pattern "\\s+",
                  :min-token-length 1,
                  :gaps true}
        props    (merge defaults params)]
    (interop/instantiate RegexTokenizer props)))
(def regex-tokenizer regex-tokeniser)
