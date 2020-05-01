(ns zero-one.geni.ml-test
  (:require
    [clojure.string :refer [includes?]]
    [midje.sweet :refer [facts fact =>]]
    [zero-one.geni.core :as g :refer [spark]]
    [zero-one.geni.dataset :as ds]
    [zero-one.geni.ml :as ml])
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

(fact "On instantiation"
  (ml/tokenizer
    {:input-col "sentence"
     :output-col "words"}) => #(instance? Tokenizer %)
  (ml/hashing-tf
    {:input-col "words"
     :output-col "rawFeatures"}) => #(instance? HashingTF %)
  (ml/word2vec
    {:vector-size 3
     :min-count 0
     :input-col "text"
     :output-col "result"}) => #(instance? Word2Vec %)
  (ml/count-vectorizer
    {:input-col "words"
     :output-col "features"}) => #(instance? CountVectorizer %)
  (ml/feature-hasher
    {:input-cols ["real" "bool" "stringNum" "string"]
     :output-col "features"}) => #(instance? FeatureHasher %)
  (ml/n-gram
    {:input-col "words"
     :output-col "features"}) => #(instance? NGram %)
  (ml/binariser
    {:threshold 0.5
     :input-col "feature"
     :output-col "binarized_feature"}) => #(instance? Binarizer %)
  (ml/pca
    {:k 3
     :input-col "features"
     :output-col "pcaFeatures"}) => #(= ((ml/params %) :k) 3)
  (ml/pca
    {:input-col "features"
     :output-col "pcaFeatures"}) => #(instance? PCA %)
  (ml/polynomial-expansion
    {:degree 3
     :input-col "features"
     :output-col "polyFeatures"}) => #(instance? PolynomialExpansion %)
  (ml/dct
    {:inverse false
     :input-col "features"
     :output-col "dctFeatures"}) => #(instance? DCT %)
  (ml/string-indexer
    {:inverse false
     :input-col "features"
     :output-col "dctFeatures"}) => #(instance? StringIndexer %)
  (ml/index-to-string
    {:input-col "category"
     :output-col "categoryIndex"}) => #(instance? IndexToString %)
  (ml/one-hot-encoder
    {:input-cols ["categoryIndex1" "categoryIndex2"]
     :output-cols ["categoryVec1" "categoryVec2"]})
  => #(instance? OneHotEncoderEstimator %)
  (ml/vector-indexer
    {:max-categories 10
     :input-col "features"
     :output-col "indexed"}) => #(instance? VectorIndexer %)
  (ml/interaction
    {:input-cols ["id2" "id3" "id4"]
     :output-col "indexed"}) => #(instance? Interaction %)
  (ml/normaliser
    {:p 1.0
     :input-col "features"
     :output-col "normFeatures"}) => #(instance? Normalizer %)
  (ml/standard-scaler
    {:with-std true
     :with-mean false
     :input-col "features"
     :output-col "scaledFeatures"}) => #(instance? StandardScaler %)
  (ml/min-max-scaler
    {:input-col "features"
     :output-col "scaledFeatures"}) => #(instance? MinMaxScaler %)
  (ml/max-abs-scaler
    {:input-col "features"
     :output-col "scaledFeatures"}) => #(instance? MaxAbsScaler %))
;; TODO: bucketiser

(facts "On pipeline"
  (fact "should be able to fit the example stages"
    (let [dataset     (ds/table->dataset
                        @g/spark
                        [[0, "a b c d e spark", 1.0]
                         [1, "b d", 0.0]
                         [2, "spark f g h", 1.0],
                         [3, "hadoop mapreduce", 0.0]]
                        [:id :text :label])
          estimator   (ml/pipeline
                        (ml/tokenizer {:input-col "text"
                                       :output-col "words"})
                        (ml/hashing-tf {:num-features 1000
                                        :input-col "words"
                                        :output-col "features"})
                        (ml/logistic-regression {:max-iter 10
                                                 :reg-param 0.001}))
          transformer (ml/fit dataset estimator)
          dtypes      (-> dataset
                          (ml/transform transformer)
                          (g/select "probability" "prediction")
                          g/dtypes)]
      (dtypes "probability") => #(includes? % "Vector")
      (dtypes "prediction") => "DoubleType"))
  (fact "should be able to fit the idf example"
    (let [dataset     (ds/table->dataset
                        @g/spark
                        [[0.0 "Hi I heard about Spark"]
                         [0.0 "I wish Java could use case classes"]
                         [1.0 "Logistic regression models are neat"]]
                        [:label :sentence])
          estimator   (ml/pipeline
                        (ml/tokenizer {:input-col "sentence"
                                       :output-col "words"})
                        (ml/hashing-tf {:num-features 20
                                        :input-col "words"
                                        :output-col "raw-features"})
                        (ml/idf {:input-col "raw-features"
                                 :output-col "features"}))
          transformer (ml/fit dataset estimator)
          transformed (-> dataset
                          (ml/transform transformer)
                          (g/select "features"))]
      (->> transformed
           g/collect-vals
           flatten
           (map ml/vector->seq)
           flatten) => #(every? double? %)))
  (fact "should be able to fit the word2vec example"
    (let [dataset     (ds/table->dataset
                        @g/spark
                        [["Hi I heard about Spark"]
                         ["I wish Java could use case classes"]
                         ["Logistic regression models are neat"]]
                        [:sentence])
          estimator   (ml/pipeline
                        (ml/tokenizer {:input-col "sentence"
                                       :output-col "text"})
                        (ml/word2vec {:vector-size 3
                                      :min-count 0
                                      :input-col "text"
                                      :output-col "result"}))
          transformer (ml/fit dataset estimator)
          transformed (-> dataset
                          (ml/transform transformer)
                          (g/select "result"))]
      (->> transformed
           g/collect-vals
           flatten
           (map ml/vector->seq)
           flatten) => #(every? double? %))))

(facts "On hypothesis testing"
  (let [dataset (ds/table->dataset
                   @spark
                   [[0.0 [0.5 10.0]]
                    [0.0 [1.5 20.0]]
                    [1.0 [1.5 30.0]]
                    [0.0 [3.5 30.0]]
                    [0.0 [3.5 40.0]]
                    [1.0 [3.5 40.0]]]
                   [:label :features])]
    (fact "able to do chi-squared test"
      (-> dataset
          (ml/chi-square-test "features" "label")
          g/first-vals
          first
          ml/vector->seq) => #(every? double? %))))

(facts "On correlation"
  (let [dataset     (ds/table->dataset
                       @spark
                       [[1.0 0.0 -2.0 0.0]
                        [4.0 5.0 0.0  3.0]
                        [6.0 7.0 0.0  8.0]
                        [9.0 0.0 1.0  0.0]]
                       [:a :b :c :d])
        v-assembler (ml/vector-assembler
                      {:input-cols ["a" "b" "c" "d"] :output-col "features"})
        features-df (-> dataset
                        (ml/transform v-assembler)
                        (g/select "features"))]
    (fact "should be able to make vectors"
      (-> features-df
          g/first-vals
          first
          ml/vector->seq) => [1.0 0.0 -2.0 0.0])
    (fact "should be able to calculate correlation"
      (let [corr-matrix (-> features-df
                            (ml/corr "features")
                            g/first-vals
                            first
                            ml/matrix->seqs)]
        (count corr-matrix) => 4
        (count (first corr-matrix)) => 4
        (every? double? (flatten corr-matrix)) => true))))

(fact "On param extraction"
  (ml/params (ml/logistic-regression {})) => {:max-iter 100,
                                              :family "auto",
                                              :tol 1.0E-6,
                                              :raw-prediction-col "rawPrediction",
                                              :elastic-net-param 0.0,
                                              :reg-param 0.0,
                                              :aggregation-depth 2,
                                              :threshold 0.5,
                                              :fit-intercept true,
                                              :label-col "label",
                                              :standardization true,
                                              :probability-col "probability",
                                              :prediction-col "prediction",
                                              :features-col "features"})
