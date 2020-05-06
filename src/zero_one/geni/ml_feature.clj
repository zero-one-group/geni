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

(defn vector-assembler [{:keys [handle-invalid input-cols output-col]
                         :or   {handle-invalid "error"}}]
  (-> (VectorAssembler.)
      (.setHandleInvalid handle-invalid)
      (.setInputCols (into-array java.lang.String input-cols))
      (.setOutputCol output-col)))

(defn feature-hasher [{:keys [num-features input-cols output-col]
                       :or   {num-features 262144}}]
  (-> (FeatureHasher.)
      (.setNumFeatures num-features)
      (.setInputCols (into-array java.lang.String input-cols))
      (.setOutputCol output-col)))

(defn n-gram [{:keys [n input-col output-col]
               :or   {n 2}}]
  (-> (NGram.)
      (.setN n)
      (.setInputCol input-col)
      (.setOutputCol output-col)))

(defn binariser [{:keys [threshold input-col output-col]
                  :or   {threshold 0.5}}]
  (-> (Binarizer.)
      (.setThreshold threshold)
      (.setInputCol input-col)
      (.setOutputCol output-col)))
(def binarizer binariser)

(defn pca [{:keys [k input-col output-col]}]
  (-> (PCA.)
      (cond-> k (.setK k))
      (.setInputCol input-col)
      (.setOutputCol output-col)))

(defn polynomial-expansion [{:keys [degree input-col output-col]
                             :or   {degree 2}}]
  (-> (PolynomialExpansion.)
      (.setDegree degree)
      (.setInputCol input-col)
      (.setOutputCol output-col)))

(defn discrete-cosine-transform [{:keys [inverse input-col output-col]
                                  :or   {inverse false}}]
  (-> (DCT.)
      (.setInverse inverse)
      (.setInputCol input-col)
      (.setOutputCol output-col)))
(def dct discrete-cosine-transform)

(defn string-indexer [{:keys [handle-invalid string-order-type input-col output-col]
                       :or   {handle-invalid "error"
                              string-order-type "frequencyDesc"}}]
  (-> (StringIndexer.)
      (.setHandleInvalid handle-invalid)
      (.setStringOrderType string-order-type)
      (.setInputCol input-col)
      (.setOutputCol output-col)))

(defn index-to-string [{:keys [input-col output-col]}]
  (-> (IndexToString.)
      (.setInputCol input-col)
      (.setOutputCol output-col)))

(defn one-hot-encoder [{:keys [drop-last handle-invalid input-cols output-cols]
                        :or   {drop-last true handle-invalid "error"}}]
  (-> (OneHotEncoderEstimator.)
      (.setDropLast drop-last)
      (.setHandleInvalid handle-invalid)
      (.setInputCols (into-array java.lang.String input-cols))
      (.setOutputCols (into-array java.lang.String output-cols))))
(def one-hot-encoder-estimator one-hot-encoder)

(defn vector-indexer [{:keys [max-categories handle-invalid input-col output-col]
                       :or   {max-categories 20 handle-invalid "error"}}]
  (-> (VectorIndexer.)
      (.setMaxCategories max-categories)
      (.setHandleInvalid handle-invalid)
      (.setInputCol input-col)
      (.setOutputCol output-col)))

(defn interaction [{:keys [input-cols output-col]}]
  (-> (Interaction.)
      (.setInputCols (into-array java.lang.String input-cols))
      (.setOutputCol output-col)))

(defn normaliser [{:keys [p input-col output-col]
                   :or   {p 2.0}}]
  (-> (Normalizer.)
      (.setP p)
      (.setInputCol input-col)
      (.setOutputCol output-col)))
(def normalizer normaliser)

(defn standard-scaler [{:keys [with-std with-mean input-col output-col]
                        :or   {with-std true with-mean false}}]
  (-> (StandardScaler.)
      (.setWithStd with-std)
      (.setWithMean with-mean)
      (.setInputCol input-col)
      (.setOutputCol output-col)))

(defn min-max-scaler [{:keys [min max input-col output-col]
                       :or   {min 0.0 max 1.0}}]
  (-> (MinMaxScaler.)
      (.setMin min)
      (.setMax max)
      (.setInputCol input-col)
      (.setOutputCol output-col)))

(defn max-abs-scaler [{:keys [input-col output-col]}]
  (-> (MaxAbsScaler.)
      (.setInputCol input-col)
      (.setOutputCol output-col)))

(defn bucketiser [{:keys [splits handle-invalid input-col output-col]
                   :or   {handle-invalid "error"}}]
  (-> (Bucketizer.)
      (cond-> splits (.setSplits (double-array splits)))
      (.setHandleInvalid handle-invalid)
      (.setInputCol input-col)
      (.setOutputCol output-col)))
(def bucketizer bucketiser)

(defn elementwise-product [{:keys [scaling-vec input-col output-col]}]
  (-> (ElementwiseProduct.)
      (cond-> scaling-vec (.setScalingVec (interop/->scala-coll scaling-vec)))
      (.setInputCol input-col)
      (.setOutputCol output-col)))

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
