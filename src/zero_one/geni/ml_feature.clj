(ns zero-one.geni.ml-feature
  (:require
    [zero-one.geni.interop :as interop])
  (:import
    (org.apache.spark.ml.feature Binarizer
                                 Bucketizer
                                 BucketedRandomProjectionLSH
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
                                 SQLTransformer
                                 StandardScaler
                                 StringIndexer
                                 Tokenizer
                                 VectorAssembler
                                 VectorIndexer
                                 VectorSizeHint
                                 Word2Vec)))

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

(defn sql-transformer [{:keys [statement]}]
  (-> (SQLTransformer.)
      (.setStatement statement)))

(defn vector-size-hint [{:keys [handle-invalid size input-col]
                         :or   {handle-invalid "error"}}]
  (-> (VectorSizeHint.)
      (.setHandleInvalid handle-invalid)
      (.setSize size)
      (.setInputCol input-col)))

(defn quantile-discretiser [{:keys [handle-invalid
                                    num-buckets
                                    relative-error
                                    input-col
                                    output-col]
                             :or   {handle-invalid "error"
                                    num-buckets 2
                                    relative-error 0.001}}]
  (-> (QuantileDiscretizer.)
      (.setHandleInvalid handle-invalid)
      (.setNumBuckets num-buckets)
      (.setRelativeError relative-error)
      (.setInputCol input-col)
      (.setOutputCol output-col)))
(def quantile-discretizer quantile-discretiser)

(defn imputer [{:keys [missing-value strategy input-cols output-cols]
                :or   {missing-value ##NaN strategy "mean"}}]
  (-> (Imputer.)
      (.setMissingValue missing-value)
      (.setStrategy strategy)
      (.setInputCols (into-array java.lang.String input-cols))
      (.setOutputCols (into-array java.lang.String output-cols))))

(defn bucketed-random-projection-lsh
  [{:keys [bucket-length num-hash-tables seed input-col output-col]
    :or   {num-hash-tables 1 seed 772209414}}]
  (-> (BucketedRandomProjectionLSH.)
      (.setBucketLength bucket-length)
      (.setNumHashTables num-hash-tables)
      (.setSeed seed)
      (.setInputCol input-col)
      (.setOutputCol output-col)))

(defn min-hash-lsh [{:keys [num-hash-tables seed input-col output-col]
                     :or   {num-hash-tables 1 seed 772209414}}]
  (-> (MinHashLSH.)
      (.setNumHashTables num-hash-tables)
      (.setSeed seed)
      (.setInputCol input-col)
      (.setOutputCol output-col)))

(defn count-vectoriser [{:keys [vocab-size
                                min-df
                                min-tf
                                binary
                                max-df
                                input-col
                                output-col]
                         :or {vocab-size 262144,
                              min-df 1.0,
                              min-tf 1.0,
                              binary false,
                              max-df 9.223372036854776E18}}]
  (-> (CountVectorizer.)
      (.setVocabSize vocab-size)
      (.setMinDF min-df)
      (.setMinTF min-tf)
      (.setBinary binary)
      (.setMaxDF max-df)
      (.setInputCol input-col)
      (.setOutputCol output-col)))

(def count-vectorizer count-vectoriser)

(defn idf [{:keys [min-doc-freq input-col output-col]
            :or   {min-doc-freq 0}}]
  (-> (IDF.)
      (.setMinDocFreq min-doc-freq)
      (.setInputCol input-col)
      (.setOutputCol output-col)))

(defn tokeniser [{:keys [input-col output-col]}]
  (-> (Tokenizer.)
      (.setInputCol input-col)
      (.setOutputCol output-col)))
(def tokenizer tokeniser)

(defn hashing-tf [{:keys [binary num-features input-col output-col]
                   :or   {binary false num-features 262144}}]
  (-> (HashingTF.)
      (.setBinary binary)
      (.setNumFeatures num-features)
      (.setInputCol input-col)
      (.setOutputCol output-col)))

(defn word2vec [{:keys [max-iter
                        step-size
                        window-size
                        max-sentence-length
                        num-partitions
                        seed
                        vector-size
                        min-count
                        input-col
                        output-col]
                 :or   {max-iter 1,
                        step-size 0.025,
                        window-size 5,
                        max-sentence-length 1000,
                        num-partitions 1,
                        seed -1961189076,
                        vector-size 100,
                        min-count 5}}]
  (-> (Word2Vec.)
      (.setMaxIter max-iter)
      (.setStepSize step-size)
      (.setWindowSize window-size)
      (.setMaxSentenceLength max-sentence-length)
      (.setNumPartitions num-partitions)
      (.setSeed seed)
      (.setVectorSize vector-size)
      (.setMinCount min-count)
      (.setInputCol input-col)
      (.setOutputCol output-col)))
