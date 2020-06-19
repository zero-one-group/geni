(ns zero-one.geni.ml-test
  (:require
    [clojure.string :refer [includes?]]
    [midje.sweet :refer [facts fact => throws]]
    [zero-one.geni.core :as g]
    [zero-one.geni.dataset :as ds]
    [zero-one.geni.ml :as ml]
    [zero-one.geni.test-resources :refer [create-temp-file!
                                          melbourne-df
                                          k-means-df
                                          libsvm-df
                                          spark]])
  (:import
    (ml.dmlc.xgboost4j.scala.spark XGBoostClassifier
                                   XGBoostRegressor)
    (org.apache.spark.ml.classification DecisionTreeClassifier
                                        GBTClassifier
                                        LinearSVC
                                        LogisticRegression
                                        MultilayerPerceptronClassifier
                                        NaiveBayes
                                        OneVsRest
                                        RandomForestClassifier)
    (org.apache.spark.ml.clustering BisectingKMeans
                                    GaussianMixture
                                    KMeans
                                    KMeansModel
                                    LDA)
    (org.apache.spark.ml.evaluation BinaryClassificationEvaluator
                                    ClusteringEvaluator
                                    MulticlassClassificationEvaluator
                                    RegressionEvaluator)
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
                                 SQLTransformer
                                 StandardScaler
                                 StopWordsRemover
                                 StringIndexer
                                 Tokenizer
                                 VectorAssembler
                                 VectorIndexer
                                 VectorSizeHint
                                 Word2Vec)
    (org.apache.spark.ml.fpm FPGrowth
                             PrefixSpan)
    (org.apache.spark.ml.recommendation ALS)
    (org.apache.spark.ml.regression AFTSurvivalRegression
                                    DecisionTreeRegressor
                                    GBTRegressor
                                    GeneralizedLinearRegression
                                    IsotonicRegression
                                    LinearRegression
                                    RandomForestRegressor)
    (org.apache.spark.sql Dataset)))

(facts "On reading and writing"
  (let [stage     (ml/vector-assembler {})
        temp-file (.toString (create-temp-file! ".xml"))]
    (ml/write-stage! stage temp-file {:overwrite true}) => nil
    (ml/write-stage! stage temp-file) => (throws Exception)
    (ml/write-stage! stage temp-file {:overwrite true
                                      :persistSubModels "true"}) => nil))

(facts "On feature extraction" :slow
  (let [indexer (ml/fit libsvm-df (ml/string-indexer {:input-col :label
                                                      :output-col :indexed-label}))]
    (ml/labels indexer) => ["1.0" "0.0"])
  (let [ds-a     (ds/table->dataset
                   spark
                   [[0 [1.0 1.0 1.0 0.0 0.0 0.0]]
                    [1 [0.0 0.0 0.0 1.0 1.0 1.0]]
                    [2 [1.0 1.0 0.0 1.0 0.0 0.0]]]
                   [:id :features])
        ds-b     (ds/table->dataset
                   spark
                   [[3 [1.0 0.0 1.0 0.0 1.0 0.0]]
                    [4 [0.0 0.0 1.0 1.0 1.0 0.0]]
                    [5 [0.0 1.0 1.0 0.0 1.0 0.0]]]
                   [:id :features])
        min-hash (ml/fit ds-a (ml/min-hash-lsh {:input-col "features"
                                                :output-col "hashes"
                                                :num-hash-tables 5}))]
    (ml/approx-nearest-neighbours
      ds-a
      min-hash
      [0.0 1.0 0.0 1.0 0.0 0.0]
      2) => #(instance? Dataset %)
    (ml/approx-nearest-neighbours
      ds-a
      min-hash
      [0.0 1.0 0.0 1.0 0.0 0.0]
      2
      "distCol") => #(instance? Dataset %)
    (ml/approx-similarity-join ds-a ds-b min-hash 0.6) => #(instance? Dataset %)
    (ml/approx-similarity-join ds-a ds-b min-hash 0.6 "JaccardDistance") => #(instance? Dataset %))
  (let [dataset   (ds/table->dataset spark [[0 ["a" "b" "c"]]] [:id :words])
        count-vec (ml/fit dataset (ml/count-vectoriser {:input-col "words"}))]
    (ml/vocabulary count-vec) => #(every? string? %))
  (let [dataset (ds/table->dataset
                  spark
                  [[[2.0  1.0]]
                   [[0.0  0.0]]
                   [[3.0 -1.0]]]
                  [:features])
        pca     (ml/fit dataset (ml/pca {:input-col "features" :k 2}))]
    (ml/principal-components pca) => #(and (seq? %) (= (count %) 2)))
  (let [dataset (ds/table->dataset
                  spark
                  [[0.0  1.0]
                   [0.0  0.0]
                   [0.0  1.0]
                   [1.0  0.0]]
                  [:i :j])
        ohe     (ml/fit
                  dataset
                  (ml/one-hot-encoder {:input-cols [:i :j]
                                       :output-cols [:x :y]}))]
    (ml/category-sizes ohe) => [2 2])
  (let [indexer (ml/fit
                  (g/limit libsvm-df 10)
                  (ml/vector-indexer {:input-col "features" :output-col "indexed"}))]
    (ml/category-maps indexer) => #(and (map? %)
                                        (every? int? (map first %))
                                        (every? map? (map second %))))
  (let [model (ml/fit
               (g/limit libsvm-df 10)
               (ml/standard-scaler {:input-col :features
                                    :with-mean true
                                    :with-std true}))]
    (ml/mean model) => #(every? double? %)
    (ml/std model) => #(every? double? %))
  (let [model (ml/fit
                (g/limit libsvm-df 10)
                (ml/min-max-scaler {:input-col "features"}))]
    (ml/original-min model) => #(every? double? %)
    (ml/original-max model) => #(every? double? %))
  (let [model (ml/fit
               (g/limit libsvm-df 10)
               (ml/max-abs-scaler {:input-col "features"}))]
    (ml/max-abs model) => #(every? double? %))
  (let [model (ml/vector-size-hint {:input-col "features" :size 111})]
    (ml/get-size model) => 111)
  (let [model (ml/fit
               (g/select melbourne-df "BuildingArea")
               (ml/imputer {:input-cols ["BuildingArea"]
                            :output-cols ["ImputedBuildingArea"]}))]
    (ml/surrogate-df model) => #(instance? Dataset %)))

(facts "On clustering" :slow
  (let [estimator   (ml/k-means {:k 3})
        model       (ml/fit k-means-df estimator)
        predictions (ml/transform k-means-df model)
        evaluator   (ml/clustering-evaluator {})
        silhoutte   (ml/evaluate predictions evaluator)]
    silhoutte => #(<= 0.6 % 1.0)
    (ml/cluster-centers model) => #(and (every? double? (flatten %))
                                        (= (count %) 3))
    (let [temp-file (.toString (create-temp-file! ".xml"))]
      (slurp temp-file) => ""
      (ml/write-stage! model temp-file {:overwrite true}) => nil
      (slurp temp-file) => #(not= % "")
      (ml/read-stage! KMeansModel temp-file) => #(instance? KMeansModel %))))

(facts "On multinomial classification" :slow
  (let [estimator   (ml/logistic-regression
                      {:thresholds [0.5 1.0]
                       :max-iter 10
                       :reg-param 0.3
                       :elastic-net-param 0.8
                       :family "multinomial"})
        model       (ml/fit libsvm-df estimator)
        predictions (-> libsvm-df
                        (ml/transform model)
                        (g/select "prediction" "label" "features"))
        evaluator   (ml/multiclass-classification-evaluator
                      {:label-col "label"
                       :prediction-col "prediction"
                       :metric-name "accuracy"})
        accuracy   (ml/evaluate predictions evaluator)]
   (fact "trainable logistic regression"
     (ml/coefficient-matrix model) => #(and (seq %)
                                            (every? seq? %)
                                            (every? double? (flatten %)))
     (ml/intercept-vector model) => #(every? double? %))
   (fact "evaluator works"
     accuracy => #(<= 0.9 % 1.0))))

(facts "On param getters"
  (let [estimator (ml/vector-assembler {:input-cols ["x" "y" "z"]})]
    (ml/input-cols estimator) => ["x" "y" "z"])
  (let [estimator (ml/one-hot-encoder {:output-cols ["c" "d"]})]
    (ml/output-cols estimator) => ["c" "d"])
  (let [estimator (ml/hashing-tf {:input-col "x" :output-col "y"})]
    (ml/input-col estimator) => "x"
    (ml/output-col estimator) => "y"))

(facts "On binary classification" :slow
  (let [estimator   (ml/logistic-regression
                      {:thresholds [0.5 1.0]
                       :max-iter 10
                       :reg-param 0.3
                       :elastic-net-param 0.8})
        model       (ml/fit libsvm-df estimator)]
   (fact "trainable binary logistic regression"
     (ml/coefficients model) => #(every? double? %)
     (ml/intercept model) => double?)
   (fact "other attributes are callable"
     (ml/binary-summary model) => (complement nil?)
     (ml/summary model) => (complement nil?)
     (ml/uid model) => string?
     (ml/num-classes model) => 2
     (ml/num-features model) => 692)
   (fact "basic param getters"
     (ml/label-col model) => "label"
     (ml/features-col model) => "features"
     (ml/prediction-col model) => "prediction"
     (ml/raw-prediction-col model) => "rawPrediction"
     (ml/probability-col model) => "probability"
     (ml/thresholds model) => [0.5 1.0])))

(facts "On decision-tree classifier" :slow
  (let [estimator   (ml/decision-tree-classifier {})
        model       (ml/fit libsvm-df estimator)]
   (fact "Attributes are callable"
     (ml/depth model) => 2
     (ml/num-nodes model) => 5
     (ml/root-node model) => (complement nil?))))

(facts "On random forest classifier" :slow
  (let [estimator   (ml/random-forest-classifier {})
        model       (ml/fit libsvm-df estimator)]
   (fact "Attributes are callable"
     (ml/feature-importances model) => #(every? double? %)
     (ml/total-num-nodes model) => int?
     (ml/trees model) => seq?)))

(facts "On gradient boosted tree classifier" :slow
  (let [estimator   (ml/gbt-classifier {:max-iter 2 :max-depth 2})
        model       (ml/fit libsvm-df estimator)]
   (fact "Attributes are callable"
     (ml/feature-importances model) => #(every? double? %)
     (ml/total-num-nodes model) => int?
     (ml/trees model) => seq?
     (ml/get-num-trees model) => int?
     (ml/tree-weights model) => #(every? double? %))))

(facts "On naive bayes classifier" :slow
  (let [estimator   (ml/naive-bayes {})
        model       (ml/fit libsvm-df estimator)]
   (fact "Attributes are callable"
     (ml/theta model) => #(and (every? seq? %)
                               (every? double? (flatten %)))
     (ml/pi model) => #(every? double? %))))

(facts "On isotonic regressor" :slow
  (let [estimator   (ml/isotonic-regression {})
        model       (ml/fit libsvm-df estimator)]
   (fact "Attributes are callable"
     (ml/boundaries model) => #(every? double? %))))

(facts "On AFT survival regression" :slow
  (let [dataset   (g/table->dataset
                     spark
                     [[1.218 1.0 [1.560 -0.605]]
                      [2.949 0.0 [0.346  2.158]]
                      [3.627 0.0 [1.380  0.231]]
                      [0.273 1.0 [0.520  1.151]]
                      [4.199 0.0 [0.795 -0.226]]]
                     [:label :censor :features])
        estimator (ml/aft-survival-regression {})
        model     (ml/fit dataset estimator)]
   (fact "Attributes are callable"
     (ml/scale model) => #(pos? %))))

(facts "On K-Means clustering" ;:slow
  (let [estimator   (ml/k-means {})
        model       (ml/fit k-means-df estimator)]
   (fact "Attributes are callable"
     (ml/cluster-centers model) => #(and (every? seq? %)
                                         (every? double? (flatten %))))))

(facts "On LDA clustering" :slow
  (let [estimator   (ml/lda {})
        model       (ml/fit k-means-df estimator)]
   (fact "Attributes are callable"
     (ml/distributed? model) => boolean?
     (ml/describe-topics model) => #(instance? Dataset %)
     (ml/estimated-doc-concentration model) => #(every? double? %)
     (ml/log-likelihood k-means-df model) => double?
     (ml/log-perplexity k-means-df model) => double?
     (ml/supported-optimisers model) => #(every? string? %)
     (ml/vocab-size model) => int?)))

(facts "On GMM clustering" :slow
  (let [estimator   (ml/gmm {})
        model       (ml/fit k-means-df estimator)]
   (fact "Attributes are callable"
     (ml/weights model) => #(every? double? %)
     (ml/gaussians-df model) => #(instance? Dataset %))))


(fact "On instantiation - XGB"
  (ml/params (ml/xgboost-regressor {:num-round 890 :max-bin 222}))
  => #(and (= (:num-round %) 890)
           (= (:max-bin %) 222))
  (ml/xgboost-regressor {})
  => #(instance? XGBoostRegressor %)
  (ml/params (ml/xgboost-classifier {:eta 0.1 :max-bin 543}))
  => #(and (= (:eta %) 0.1)
           (= (:max-bin %) 543))
  (ml/xgboost-classifier {})
  => #(instance? XGBoostClassifier %))

(fact "On instantiation - FPM"
  (ml/params (ml/prefix-span {:max-pattern-length 321}))
  => #(= (:max-pattern-length %) 321)
  (ml/prefix-span {})
  => #(instance? PrefixSpan %)

  (ml/params (ml/frequent-pattern-growth {:min-support 0.12345}))
  => #(= (:min-support %) 0.12345)
  (ml/fp-growth {})
  => #(instance? FPGrowth %))

(fact "On instantiation - recommendation"
  (ml/params (ml/als {:num-user-blocks 12345}))
  => #(= (:num-user-blocks %) 12345)
  (ml/alternating-least-squares {})
  => #(instance? ALS %))

(fact "On instantiation - clustering"
  (ml/params (ml/gaussian-mixture {:features-col "fts"}))
  => #(= (:features-col %) "fts")
  (ml/gmm {})
  => #(instance? GaussianMixture %)

  (ml/params (ml/bisecting-k-means {:distance-measure "cosine"}))
  => #(= (:distance-measure %) "cosine")
  (ml/bisecting-k-means {})
  => #(instance? BisectingKMeans %)

  (ml/params (ml/lda {:optimizer "em"}))
  => #(= (:optimizer %) "em")
  (ml/latent-dirichlet-allocation {})
  => #(instance? LDA %)

  (ml/params (ml/k-means {:k 123}))
  => #(= (:k %) 123)
  (ml/k-means {})
  => #(instance? KMeans %))

(fact "On instantiation - evaluator"
  (ml/params (ml/binary-classification-evaluator {:raw-prediction-col "xyz"}))
  => #(= (:raw-prediction-col %) "xyz")
  (ml/binary-classification-evaluator {})
  => #(instance? BinaryClassificationEvaluator %)

  (ml/params (ml/clustering-evaluator {:distance-measure "cosine"}))
  => #(= (:distance-measure %) "cosine")
  (ml/clustering-evaluator {})
  => #(instance? ClusteringEvaluator %)

  (ml/params (ml/multiclass-classification-evaluator {:label-col "weightz"}))
  => #(= (:label-col %) "weightz")
  (ml/multiclass-classification-evaluator {})
  => #(instance? MulticlassClassificationEvaluator %)

  (ml/params (ml/regression-evaluator {:metric-name "r2"}))
  => #(= (:metric-name %) "r2")
  (ml/regression-evaluator {})
  => #(instance? RegressionEvaluator %))

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
  (ml/params (ml/logistic-regression {:thresholds [0.0 0.1]}))
  => #(= (:thresholds %) [0.0 0.1])
  (ml/logistic-regression {})
  => #(instance? LogisticRegression %)

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
  (ml/params (ml/stop-words-remover {:case-sensitive true}))
  => #(= (:case-sensitive %) true)
  (ml/stop-words-remover {})
  => #(and (instance? StopWordsRemover %))
  (-> (ml/stop-words-remover {}) ml/params :stop-words count)
  => 181

  (ml/params (ml/chi-sq-selector {:num-top-features 1122}))
  => #(= (:num-top-features %) 1122)
  (ml/chi-sq-selector {})
  => #(instance? ChiSqSelector %)

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
  => #(instance? OneHotEncoder %)

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
  => #(instance? Word2Vec %)

  (ml/params (ml/regex-tokeniser {:pattern "\\W"}))
  => #(= (:pattern %) "\\W")
  (ml/regex-tokenizer {})
  => #(instance? RegexTokenizer %))

(facts "On pipeline" :slow
  (fact "should be able to fit the example stages" :slow
    (let [dataset     (ds/table->dataset
                        spark
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
      (:probability dtypes) => #(includes? % "Vector")
      (:prediction dtypes) => "DoubleType"))
  (fact "should be able to fit the idf example" :slow
    (let [dataset     (ds/table->dataset
                        spark
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
      (->> transformed g/collect-vals flatten) => #(every? double? %)
      (-> transformer ml/stages last ml/idf-vector) => #(every? double? %)))
  (fact "should be able to fit the word2vec example" :slow
    (let [dataset     (ds/table->dataset
                        spark
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
      (->> transformed g/collect-vals flatten) => #(every? double? %))))

(facts "On hypothesis testing"
  (let [dataset (ds/table->dataset
                   spark
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
          first) => #(every? double? %))))

(facts "On correlation" :slow
  (let [dataset     (ds/table->dataset
                       spark
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
      (-> features-df g/first-vals first) => [1.0 0.0 -2.0 0.0])
    (fact "should be able to calculate correlation"
      (let [corr-matrix (-> features-df
                            (ml/corr "features")
                            g/first-vals
                            first)]
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
