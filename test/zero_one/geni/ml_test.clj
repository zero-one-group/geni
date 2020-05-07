(ns zero-one.geni.ml-test
  (:require
    [clojure.string :refer [includes?]]
    [midje.sweet :refer [facts fact =>]]
    [zero-one.geni.core :as g :refer [spark]]
    [zero-one.geni.dataset :as ds]
    [zero-one.geni.ml :as ml]
    [zero-one.geni.interop :refer [vector->seq matrix->seqs]])
  (:import
    (org.apache.spark.ml.classification DecisionTreeClassifier
                                        GBTClassifier
                                        LinearSVC
                                        LogisticRegression
                                        MultilayerPerceptronClassifier
                                        NaiveBayes
                                        OneVsRest
                                        RandomForestClassifier)
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
                                 Word2Vec)
    (org.apache.spark.ml.regression AFTSurvivalRegression
                                    DecisionTreeRegressor
                                    GBTRegressor
                                    GeneralizedLinearRegression
                                    IsotonicRegression
                                    LinearRegression
                                    RandomForestRegressor)))

;; TODO: put all data into one namespace
(defonce libsvm-df
  (-> @g/spark
      .read
      (.format "libsvm")
      (.load "test/resources/sample_libsvm_data.txt")))

(facts "On classification"
  (fact "trainable logistic regression"
    (let [estimator (ml/logistic-regression
                      {:thresholds [0.0 1.0]
                       :max-iter 10
                       :reg-param 0.3
                       :elastic-net-param 0.8
                       :family "multinomial"})
          model (ml/fit libsvm-df estimator)]
     (vector->seq (.coefficientMatrix model)) => #(every? double? %)
     (vector->seq (.interceptVector model)) => #(every? double? %))))

(fact "On instantiation - regression"
  (ml/params (ml/isotonic-regression {:label-col "ABC"}))
  => #(= (:label-col %) "ABC")
  (ml/isotonic-regression {})
  => #(instance? IsotonicRegression %)

  (ml/params (ml/aft-survival-regression {:quantile-probabilities [0.005 0.995]}))
  => #(= (:quantile-probabilities %) [0.005 0.995])
  (ml/aft-survival-regression {})
  => #(instance? AFTSurvivalRegression %)

  (ml/params (ml/gbt-regressor {:max-bins 128}))
  => #(= (:max-bins %) 128)
  (ml/gbt-regressor {})
  => #(instance? GBTRegressor %)

  (ml/params (ml/random-forest-regressor {:prediction-col "xyz"}))
  => #(= (:prediction-col %) "xyz")
  (ml/random-forest-regressor {})
  => #(instance? RandomForestRegressor %)

  (ml/params (ml/decision-tree-regressor {:variance-col "abc"}))
  => #(= (:variance-col %) "abc")
  (ml/decision-tree-regressor {})
  => #(instance? DecisionTreeRegressor %)

  (ml/params (ml/glm {:reg-param 1.0}))
  => #(= (:reg-param %) 1.0)
  (ml/generalized-linear-regression {})
  => #(instance? GeneralizedLinearRegression %)

  (ml/params (ml/linear-regression {:standardisation false}))
  => #(= (:standardization %) false)
  (ml/linear-regression {})
  => #(instance? LinearRegression %))

(fact "On instantiation - classification"
  (ml/params (ml/naive-bayes {:thresholds [0.0 0.1]}))
  => #(= (:thresholds %) [0.0 0.1])
  (ml/naive-bayes {})
  => #(instance? NaiveBayes %)

  (let [classifier (ml/logistic-regression {:max-iter 10 :tol 1e-6})]
    (ml/params (ml/one-vs-rest {:classifier classifier}))
    => #(instance? LogisticRegression (:classifier %)))
  (ml/one-vs-rest {})
  => #(instance? OneVsRest %)

  (ml/params (ml/linear-svc {:standardisation false}))
  => #(= (:standardization %) false)
  (ml/linear-svc {})
  => #(instance? LinearSVC %)

  (ml/params (ml/mlp-classifier {:layers [1 2 3]}))
  => #(= (:layers %) [1 2 3])
  (ml/mlp-classifier {})
  => #(instance? MultilayerPerceptronClassifier %)

  (ml/params (ml/gbt-classifier {:feature-subset-strategy "auto"}))
  => #(= (:feature-subset-strategy %) "auto")
  (ml/gbt-classifier {})
  => #(instance? GBTClassifier %)

  (ml/params (ml/random-forest-classifier {:num-trees 12}))
  => #(= (:num-trees %) 12)
  (ml/random-forest-classifier {})
  => #(instance? RandomForestClassifier %)

  (ml/params (ml/decision-tree-classifier {:thresholds [0.0]}))
  => #(= (:thresholds %) [0.0])
  (ml/decision-tree-classifier {})
  => #(instance? DecisionTreeClassifier %))

(fact "On instantiation - features"
  (ml/params (ml/vector-assembler {:handle-invalid "skip"}))
  => #(= (:handle-invalid %) "skip")
  (ml/vector-assembler {})
  => #(instance? VectorAssembler %)

  (ml/params (ml/feature-hasher {:input-cols ["real" "bool" "stringNum" "string"]}))
  => #(= (:input-cols %) ["real" "bool" "stringNum" "string"])
  (ml/feature-hasher {})
  => #(instance? FeatureHasher %)

  (ml/params (ml/n-gram {:input-col "words"}))
  => #(= (:input-col %) "words")
  (ml/n-gram {})
  => #(instance? NGram %)

  (ml/params (ml/binariser {:threshold 0.5}))
  => #(= (:threshold %) 0.5)
  (ml/binarizer {})
  => #(instance? Binarizer %)

  (ml/params (ml/pca {:k 3}))
  => #(= (:k %) 3)
  (ml/pca {})
  => #(instance? PCA %)

  (ml/params (ml/polynomial-expansion {:degree 3}))
  => #(= (:degree %) 3)
  (ml/polynomial-expansion {})
  => #(instance? PolynomialExpansion %)

  (ml/params (ml/dct {:inverse true}))
  => #(= (:inverse %) true)
  (ml/discrete-cosine-transform {})
  => #(instance? DCT %)

  (ml/params (ml/string-indexer {:handle-invalid "skip"}))
  => #(= (:handle-invalid %) "skip")
  (ml/string-indexer {})
  => #(instance? StringIndexer %)

  (ml/params (ml/index-to-string {:output-col "categoryIndex"}))
  => #(= (:output-col %) "categoryIndex")
  (ml/index-to-string {})
  => #(instance? IndexToString %)

  (ml/params (ml/one-hot-encoder {:input-cols ["categoryIndex1" "categoryIndex2"]}))
  => #(= (:input-cols %) ["categoryIndex1" "categoryIndex2"])
  (ml/one-hot-encoder {})
  => #(instance? OneHotEncoderEstimator %)

  (ml/params (ml/vector-indexer {:max-categories 10}))
  => #(= (:max-categories %) 10)
  (ml/vector-indexer {})
  => #(instance? VectorIndexer %)

  (ml/params (ml/interaction {:output-col "indexed"}))
  => #(= (:output-col %) "indexed")
  (ml/interaction {})
  => #(instance? Interaction %)

  (ml/params (ml/normaliser {:p 1.0}))
  => #(= (:p %) 1.0)
  (ml/normalizer {})
  => #(instance? Normalizer %)

  (ml/params (ml/standard-scaler {:input-col "abcdef"}))
  => #(= (:input-col %) "abcdef")
  (ml/standard-scaler {})
  => #(instance? StandardScaler %)

  (ml/params (ml/min-max-scaler {:min -9999}))
  => #(= (:min %) -9999.0)
  (ml/min-max-scaler {})
  => #(instance? MinMaxScaler %)

  (ml/params (ml/max-abs-scaler {:output-col "xyz"}))
  => #(= (:output-col %) "xyz")
  (ml/max-abs-scaler {})
  => #(instance? MaxAbsScaler %)

  (ml/params (ml/bucketiser {:splits [-999.9 -0.5 -0.3 0.0 0.2 999.9]}))
  => #(= (:splits %) [-999.9 -0.5 -0.3 0.0 0.2 999.9])
  (ml/bucketiser {})
  => #(instance? Bucketizer %)

  (ml/params (ml/elementwise-product {:scaling-vec [0.0 1.0 2.0]}))
  => #(= (:scaling-vec %) [0.0 1.0 2.0])
  (ml/elementwise-product {})
  => #(instance? ElementwiseProduct %)

  (ml/params (ml/sql-transformer {:statement "SELECT *, (v1 + v2)"}))
  => #(= (:statement %) "SELECT *, (v1 + v2)")
  (ml/sql-transformer {})
  => #(instance? SQLTransformer %)

  (ml/params (ml/vector-size-hint {:size 3}))
  => #(= (:size %) 3)
  (ml/vector-size-hint {})
  => #(instance? VectorSizeHint %)

  (ml/params (ml/quantile-discretiser {:num-buckets 3}))
  => #(= (:num-buckets %) 3)
  (ml/quantile-discretizer {})
  => #(instance? QuantileDiscretizer %)

  (ml/params (ml/imputer {:input-cols ["a" "b"]}))
  => #(= (:input-cols %) ["a" "b"])
  (ml/imputer {})
  => #(instance? Imputer %)

  (ml/params (ml/bucketed-random-projection-lsh {:bucket-length 2.0}))
  => #(= (:bucket-length %) 2.0)
  (ml/bucketed-random-projection-lsh {})
  => #(instance? BucketedRandomProjectionLSH %)

  (ml/params (ml/min-hash-lsh {:num-hash-tables 55}))
  => #(= (:num-hash-tables %) 55)
  (ml/min-hash-lsh {})
  => #(instance? MinHashLSH %)

  (ml/params (ml/count-vectoriser {:min-df 2.0 :min-tf 3.0 :max-df 4.0}))
  => #(and (= (:min-df %) 2.0)
           (= (:min-tf %) 3.0)
           (= (:max-df %) 4.0))
  (ml/count-vectorizer {})
  => #(instance? CountVectorizer %)

  (ml/params (ml/idf {:min-doc-freq 100}))
  => #(= (:min-doc-freq %) 100)
  (ml/idf {})
  => #(instance? IDF %)

  (ml/params (ml/tokeniser {:input-col "sentence"}))
  => #(= (:input-col %) "sentence")
  (ml/tokenizer {})
  => #(instance? Tokenizer %)

  (ml/params (ml/hashing-tf {:output-col "rawFeatures"}))
  => #(= (:output-col %) "rawFeatures")
  (ml/hashing-tf {})
  => #(instance? HashingTF %)

  (ml/params (ml/word2vec {:vector-size 3}))
  => #(= (:vector-size %) 3)
  (ml/word2vec {})
  => #(instance? Word2Vec %))

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
           (map vector->seq)
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
           (map vector->seq)
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
          vector->seq) => #(every? double? %))))

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
          vector->seq) => [1.0 0.0 -2.0 0.0])
    (fact "should be able to calculate correlation"
      (let [corr-matrix (-> features-df
                            (ml/corr "features")
                            g/first-vals
                            first
                            matrix->seqs)]
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
