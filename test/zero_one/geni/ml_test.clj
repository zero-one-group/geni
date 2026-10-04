(ns ^:classic zero-one.geni.ml-test
  (:require
   [clojure.string :refer [includes?]]
   [clojure.test :refer [deftest is testing]]
   [zero-one.geni.core :as g]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.ml :as ml]
   [zero-one.geni.spark :as geni-spark]
   [zero-one.geni.test-resources :refer [create-temp-file!
                                         df-20
                                         melbourne-df
                                         k-means-df
                                         libsvm-df
                                         spark
                                         stop-session!]])
  (:import
   (org.apache.spark.ml.classification DecisionTreeClassifier
                                       FMClassifier
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
                                   LDA
                                   PowerIterationClustering)
   (org.apache.spark.ml.evaluation BinaryClassificationEvaluator
                                   ClusteringEvaluator
                                   MulticlassClassificationEvaluator
                                   MultilabelClassificationEvaluator
                                   RankingEvaluator
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
                                RFormula
                                RegexTokenizer
                                RobustScaler
                                SQLTransformer
                                StandardScaler
                                StopWordsRemover
                                StringIndexer
                                Tokenizer
                                UnivariateFeatureSelector
                                VarianceThresholdSelector
                                VectorAssembler
                                VectorIndexer
                                VectorSizeHint
                                VectorSlicer
                                Word2Vec)
   (org.apache.spark.ml.fpm FPGrowth
                            PrefixSpan)
   (org.apache.spark.ml.recommendation ALS)
   (org.apache.spark.ml.regression AFTSurvivalRegression
                                   DecisionTreeRegressor
                                   FMRegressor
                                   GBTRegressor
                                   GeneralizedLinearRegression
                                   IsotonicRegression
                                   LinearRegression
                                   RandomForestRegressor)
   (org.apache.spark.sql Dataset)))

(defn- spark-4? []
  (= "4" (first (re-seq #"\d+" (geni-spark/classpath-version)))))

(deftest reading-and-writing-test
  (let [stage     (ml/vector-assembler {})
        temp-file (.toString (create-temp-file! ".xml"))]
    (is (nil? (ml/write-stage! stage temp-file {:mode "overwrite"})))
    (is (thrown? Exception (ml/write-stage! stage temp-file)))
    (is (nil? (ml/write-stage! stage temp-file {:mode "overwrite"
                                                :persistSubModels "true"})))))

(definterface ColSetters
  (^Object setCol [^String col])
  (^Object setCol [^"[Ljava.lang.String;" cols]))

(def ^:private set-cols (atom []))

(deftype ColStage []
  ColSetters
  (^Object setCol [_ ^String col]
    (swap! set-cols conj [:one col])
    nil)
  (^Object setCol [_ ^"[Ljava.lang.String;" cols]
    (swap! set-cols conj [:many (vec cols)])
    nil))

(deftest overloaded-setters-test
  (testing "a param with overloaded setters, as XGBoost's :features-col, gets the one that fits"
    (reset! set-cols [])
    (interop/instantiate ColStage {:col "a"})
    (interop/instantiate ColStage {:col ["a" "b"]})
    (is (= [[:one "a"] [:many ["a" "b"]]] @set-cols))))

(deftest params-test
  (testing "an unknown param throws, with the closest one as a suggestion"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Tokenizer has no param :inptu-col\. Did you mean :input-col\?"
                          (ml/tokenizer {:inptu-col :x})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Tokenizer has no param :typo\. Its params are :input-col, :output-col\."
                          (ml/tokenizer (array-map :input-col :x :typo true)))))
  (testing "the instance comes back, whichever param is set last"
    (is (instance? Tokenizer (ml/tokenizer (array-map :input-col :x :output-col :y))))
    (is (instance? Tokenizer (ml/tokenizer (array-map :output-col :y :input-col :x)))))
  (testing "a default that the class has no setter for is skipped"
    (is (instance? Tokenizer (interop/instantiate Tokenizer {:no-such-param 1} {:input-col "x"}))))
  (testing "the British spelling of standardisation still works"
    (is (false? (.getStandardization (ml/logistic-regression {:standardisation false}))))
    (is (false? (.getStandardization (ml/linear-regression {:standardisation false}))))))

(deftest ^:slow stages-without-a-session-test
  (testing "write-stage! and read-stage! use Geni's default session, rather than Spark's
            getOrCreate, which needs a master URL"
    (let [temp-file (.toString (create-temp-file! ".stage"))
          stage     (ml/vector-assembler {:input-cols [:a :b] :output-col :v})]
      (stop-session!)
      (ml/write-stage! stage temp-file {:mode "overwrite"})
      (stop-session!)
      (is (= ["a" "b"] (seq (.getInputCols (ml/read-stage! VectorAssembler temp-file))))))))

(deftest ^:slow feature-extraction-test
  (let [indexer (ml/fit (libsvm-df) (ml/string-indexer {:input-col :label
                                                        :output-col :indexed-label}))]
    (is (= ["1.0" "0.0"] (ml/labels indexer))))
  (let [ds-a     (g/table->dataset
                  @spark
                  [[0 (g/dense 1.0 1.0 1.0 0.0 0.0 0.0)]
                   [1 (g/dense 0.0 0.0 0.0 1.0 1.0 1.0)]
                   [2 (g/dense 1.0 1.0 0.0 1.0 0.0 0.0)]]
                  [:id :features])
        ds-b     (g/table->dataset
                  @spark
                  [[3 (g/dense 1.0 0.0 1.0 0.0 1.0 0.0)]
                   [4 (g/dense 0.0 0.0 1.0 1.0 1.0 0.0)]
                   [5 (g/dense 0.0 1.0 1.0 0.0 1.0 0.0)]]
                  [:id :features])
        min-hash (ml/fit ds-a (ml/min-hash-lsh {:input-col "features"
                                                :output-col "hashes"
                                                :num-hash-tables 5}))]
    (is (instance? Dataset (ml/approx-nearest-neighbours
                            ds-a
                            min-hash
                            [0.0 1.0 0.0 1.0 0.0 0.0]
                            2)))
    (is (instance? Dataset (ml/approx-nearest-neighbours
                            ds-a
                            min-hash
                            [0.0 1.0 0.0 1.0 0.0 0.0]
                            2
                            "distCol")))
    (is (instance? Dataset (ml/approx-similarity-join ds-a ds-b min-hash 0.6)))
    (is (instance? Dataset (ml/approx-similarity-join ds-a ds-b min-hash 0.6 "JaccardDistance"))))
  (let [dataset   (g/table->dataset
                   @spark
                   [[0 ["a" "b" "c"]]] [:id :words])
        count-vec (ml/fit dataset (ml/count-vectoriser {:input-col "words"}))]
    (is (every? string? (ml/vocabulary count-vec))))
  (let [dataset (g/table->dataset
                 @spark
                 [[(g/dense 2.0  1.0)]
                  [(g/dense 0.0  0.0)]
                  [(g/dense 3.0 -1.0)]]
                 [:features])
        pca     (ml/fit dataset (ml/pca {:input-col "features" :k 2}))
        actual  (ml/principal-components pca)]
    (is (and (seq? actual) (= (count actual) 2))))
  (let [dataset (g/table->dataset
                 @spark
                 [[0.0  1.0]
                  [0.0  0.0]
                  [0.0  1.0]
                  [1.0  0.0]]
                 [:i :j])
        ohe     (ml/fit
                 dataset
                 (ml/one-hot-encoder {:input-cols [:i :j]
                                      :output-cols [:x :y]}))]
    (is (= [2 2] (ml/category-sizes ohe))))
  (let [indexer (ml/fit
                 (g/limit (libsvm-df) 10)
                 (ml/vector-indexer {:input-col "features" :output-col "indexed"}))
        actual  (ml/category-maps indexer)]
    (is (and (map? actual)
             (every? int? (map first actual))
             (every? map? (map second actual)))))
  (let [model (ml/fit
               (g/limit (libsvm-df) 10)
               (ml/standard-scaler {:input-col :features
                                    :with-mean true
                                    :with-std true}))]
    (is (every? double? (ml/mean model)))
    (is (every? double? (ml/std model))))
  (let [model (ml/fit
               (g/limit (libsvm-df) 10)
               (ml/min-max-scaler {:input-col "features"}))]
    (is (every? double? (ml/original-min model)))
    (is (every? double? (ml/original-max model))))
  (let [model (ml/fit
               (g/limit (libsvm-df) 10)
               (ml/max-abs-scaler {:input-col "features"}))]
    (is (every? double? (ml/max-abs model))))
  (let [model (ml/vector-size-hint {:input-col "features" :size 111})]
    (is (= 111 (ml/get-size model))))
  (let [model (ml/fit
               (g/select (melbourne-df) "BuildingArea")
               (ml/imputer {:input-cols ["BuildingArea"]
                            :output-cols ["ImputedBuildingArea"]}))]
    (is (instance? Dataset (ml/surrogate-df model)))))

(deftest ^:slow clustering-test
  (let [estimator   (ml/k-means {:k 3})
        model       (ml/fit (k-means-df) estimator)
        predictions (ml/transform (k-means-df) model)
        evaluator   (ml/clustering-evaluator {})
        silhoutte   (ml/evaluate predictions evaluator)]
    (is (<= 0.6 silhoutte 1.0))
    (let [actual (ml/cluster-centers model)]
      (is (and (every? double? (flatten actual))
               (= (count actual) 3))))
    (let [temp-file (.toString (create-temp-file! ".xml"))]
      (is (= "" (slurp temp-file)))
      (is (nil? (ml/write-stage! model temp-file {:mode "overwrite"})))
      (is (seq (.list (java.io.File. temp-file))))
      (is (instance? KMeansModel (ml/read-stage! KMeansModel temp-file))))))

(deftest ^:slow multinomial-classification-test
  (let [estimator   (ml/logistic-regression
                     {:thresholds [0.5 1.0]
                      :max-iter 10
                      :reg-param 0.3
                      :elastic-net-param 0.8
                      :family "multinomial"})
        model       (ml/fit (libsvm-df) estimator)
        predictions (-> (libsvm-df)
                        (ml/transform model)
                        (g/select "prediction" "label" "features"))
        evaluator   (ml/multiclass-classification-evaluator
                     {:label-col "label"
                      :prediction-col "prediction"
                      :metric-name "accuracy"})
        accuracy   (ml/evaluate predictions evaluator)]
    (testing "trainable logistic regression"
      (let [actual (ml/coefficient-matrix model)]
        (is (and (seq actual)
                 (every? seq? actual)
                 (every? double? (flatten actual)))))
      (is (every? double? (ml/intercept-vector model))))
    (testing "evaluator works"
      (is (<= 0.9 accuracy 1.0)))))

(deftest param-getters-test
  (let [estimator (ml/vector-assembler {:input-cols ["x" "y" "z"]})]
    (is (= ["x" "y" "z"] (ml/input-cols estimator))))
  (let [estimator (ml/one-hot-encoder {:output-cols ["c" "d"]})]
    (is (= ["c" "d"] (ml/output-cols estimator))))
  (let [estimator (ml/hashing-tf {:input-col "x" :output-col "y"})]
    (is (= "x" (ml/input-col estimator)))
    (is (= "y" (ml/output-col estimator)))))

(deftest ^:slow binary-classification-test
  (let [estimator   (ml/logistic-regression
                     {:thresholds [0.5 1.0]
                      :max-iter 10
                      :reg-param 0.3
                      :elastic-net-param 0.8})
        model       (ml/fit (libsvm-df) estimator)]
    (testing "trainable binary logistic regression"
      (is (every? double? (ml/coefficients model)))
      (is (double? (ml/intercept model))))
    (testing "other attributes are callable"
      (is (not (nil? (ml/binary-summary model))))
      (is (not (nil? (ml/summary model))))
      (is (string? (ml/uid model)))
      (is (= 2 (ml/num-classes model)))
      (is (= 780 (ml/num-features model))))
    (testing "basic param getters"
      (is (= "label" (ml/label-col model)))
      (is (= "features" (ml/features-col model)))
      (is (= "prediction" (ml/prediction-col model)))
      (is (= "rawPrediction" (ml/raw-prediction-col model)))
      (is (= "probability" (ml/probability-col model)))
      (is (= [0.5 1.0] (ml/thresholds model))))))

(deftest ^:slow decision-tree-classifier-test
  (let [estimator   (ml/decision-tree-classifier {})
        model       (ml/fit (libsvm-df) estimator)]
    (testing "Attributes are callable"
      (is (= 2 (ml/depth model)))
      (is (= 5 (ml/num-nodes model)))
      (is (not (nil? (ml/root-node model)))))))

(deftest ^:slow random-forest-classifier-test
  (let [estimator   (ml/random-forest-classifier {:num-trees 2 :max-depth 2})
        model       (ml/fit (libsvm-df) estimator)]
    (testing "Attributes are callable"
      (is (every? double? (:values (ml/feature-importances model))))
      (is (int? (ml/total-num-nodes model)))
      (is (seq? (ml/trees model))))))

(deftest ^:slow gradient-boosted-tree-classifier-test
  (let [estimator   (ml/gbt-classifier {:max-iter 2 :max-depth 2})
        model       (ml/fit (libsvm-df) estimator)]
    (testing "Attributes are callable"
      (is (every? double? (:values (ml/feature-importances model))))
      (is (int? (ml/total-num-nodes model)))
      (is (seq? (ml/trees model)))
      (is (int? (ml/get-num-trees model)))
      (is (every? double? (ml/tree-weights model))))))

(deftest ^:slow naive-bayes-classifier-test
  (let [estimator   (ml/naive-bayes {})
        model       (ml/fit (libsvm-df) estimator)]
    (testing "Attributes are callable"
      (let [actual (ml/theta model)]
        (is (and (every? seq? actual)
                 (every? double? (flatten actual)))))
      (is (every? double? (ml/pi model))))))

(deftest ^:slow isotonic-regressor-test
  (let [estimator   (ml/isotonic-regression {})
        model       (ml/fit (libsvm-df) estimator)]
    (testing "Attributes are callable"
      (is (every? double? (ml/boundaries model))))))

(deftest ^:slow aft-survival-regression-test
  (let [dataset   (g/table->dataset
                   @spark
                   [[1.218 1.0 (g/dense [1.560 -0.605])]
                    [2.949 0.0 (g/dense [0.346  2.158])]
                    [3.627 0.0 (g/dense [1.380  0.231])]
                    [0.273 1.0 (g/dense [0.520  1.151])]
                    [4.199 0.0 (g/dense [0.795 -0.226])]]
                   [:label :censor :features])
        estimator (ml/aft-survival-regression {})
        model     (ml/fit dataset estimator)]
    (testing "Attributes are callable"
      (is (pos? (ml/scale model))))))

(deftest ^:slow k-means-clustering-test
  (let [estimator   (ml/k-means {:max-iter 2})
        model       (ml/fit (k-means-df) estimator)]
    (testing "Attributes are callable"
      (let [actual (ml/cluster-centers model)]
        (is (and (every? seq? actual)
                 (every? double? (flatten actual))))))))

(deftest ^:slow lda-clustering-test
  (let [estimator   (ml/lda {:max-iter 2})
        model       (ml/fit (k-means-df) estimator)]
    (testing "Attributes are callable"
      (is (boolean? (ml/distributed? model)))
      (is (instance? Dataset (ml/describe-topics model)))
      (is (every? double? (ml/estimated-doc-concentration model)))
      (is (double? (ml/log-likelihood (k-means-df) model)))
      (is (double? (ml/log-perplexity (k-means-df) model)))
      (is (every? string? (ml/supported-optimisers model)))
      (is (int? (ml/vocab-size model))))))

(deftest ^:slow gmm-clustering-test
  (let [estimator   (ml/gmm {:max-iter 2})
        model       (ml/fit (k-means-df) estimator)]
    (testing "Attributes are callable"
      (is (every? double? (ml/weights model)))
      (is (instance? Dataset (ml/gaussians-df model))))))

(defn- first-features
  "The first row's features in libsvm-df, an MLlib vector."
  []
  (-> (libsvm-df) (g/select :features) .first (.get 0)))

(defn- small-df
  "Eight rows of a label and three non-negative features, which fit quickly."
  []
  (g/table->dataset @spark
                    (for [[label a b c] [[0.0 1.0 0.0 2.0] [0.0 2.0 1.0 1.0] [0.0 1.0 1.0 3.0] [0.0 2.0 0.0 2.0]
                                         [1.0 7.0 5.0 0.0] [1.0 8.0 6.0 1.0] [1.0 9.0 5.0 0.0] [1.0 8.0 7.0 1.0]]]
                      [label (g/dense a b c)])
                    [:label :features]))

(deftest ^:slow feature-models-test
  (let [df (g/table->dataset @spark
                             [[1.0 "a" 2.0 0.0 0] [0.0 "b" 1.0 0.0 1] [1.0 "a" 3.0 0.0 0] [0.0 "c" 0.5 0.0 2]]
                             [:y :t :x :z :cat])
        assembled (ml/transform df (ml/vector-assembler {:input-cols [:x :z :y] :output-col :features}))]
    (testing "r-formula, and its resolved formula"
      (let [model (ml/fit df (ml/r-formula {:formula "y ~ t + x"}))]
        (is (= [[[1.0 0.0 2.0] 1.0] [[0.0 1.0 1.0] 0.0]]
               (take 2 (g/collect-vals (g/select (ml/transform df model) :features :label)))))
        (is (= "ResolvedRFormula(label=y, terms=[t,x], hasIntercept=true)"
               (ml/resolved-formula-string model)))))
    (testing "selectors and the features they keep"
      (is (= [0 2] (ml/selected-features
                    (ml/fit assembled (ml/variance-threshold-selector {:output-col :selected})))))
      (is (= [0] (ml/selected-features
                  (ml/fit assembled (ml/univariate-feature-selector {:label-col :y
                                                                     :output-col :selected
                                                                     :feature-type "continuous"
                                                                     :label-type "categorical"
                                                                     :selection-threshold 1}))))))
    (testing "vector-slicer"
      (is (= [[2.0 1.0] [1.0 0.0]]
             (->> (ml/transform assembled (ml/vector-slicer {:input-col :features :output-col :s :indices [0 2]}))
                  g/collect-vals
                  (map last)
                  (take 2)))))
    (testing "target-encoder, on Spark 4"
      (when (spark-4?)
        (is (= [1.0 0.0 1.0 0.0]
               (-> df
                   (ml/transform (ml/fit df (ml/target-encoder {:input-cols [:cat] :output-cols [:encoded]
                                                                :label-col :y :target-type "binary"})))
                   (g/collect-col :encoded)))))))
  (testing "models from known labels and a known vocabulary"
    (is (= [0.0 1.0 2.0]
           (-> (g/table->dataset @spark [["low"] ["high"] ["mid"]] [:level])
               (ml/transform (ml/string-indexer-model {:labels ["low" "high" "mid"] :input-col :level :output-col :i}))
               (g/collect-col :i))))
    (is (= [["a" "b"] ["c"]]
           (ml/labels-array (ml/string-indexer-model {:labels-array [["a" "b"] ["c"]]
                                                      :input-cols [:p :q] :output-cols [:pi :qi]}))))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"either :labels or :labels-array"
                          (ml/string-indexer-model {:input-col :x})))
    (is (= [{:size 2 :indices [0 1] :values [2.0 1.0]}]
           (-> (g/table->dataset @spark [[["a" "b" "a" "z"]]] [:words])
               (ml/transform (ml/count-vectorizer-model {:vocabulary ["a" "b"] :input-col :words :output-col :counts}))
               (g/collect-col :counts)))))
  (testing "Spark's stop words by language"
    (is (= ["au" "aux" "avec"] (take 3 (ml/load-default-stop-words :french)))))
  (testing "arrays to vectors and back"
    (is (= [[1.0 2.0]]
           (-> (g/table->dataset @spark [[[1.0 2.0]]] [:a])
               (g/select {:v (ml/array->vector :a)})
               (g/select {:a (ml/vector->array :v)})
               (g/collect-col :a))))))

(deftest ^:slow model-accessors-test
  (let [features (first-features)
        libsvm   (g/limit (libsvm-df) 60)]
    (testing "one row's predictions"
      (let [model (ml/fit libsvm (ml/logistic-regression {:max-iter 5}))]
        (is (= 0.0 (ml/predict model features)))
        (is (= 1.0 (ml/predict model (vec (repeat 780 0.0)))))
        (is (= 2 (count (ml/predict-raw model features))))
        (is (< 0.99 (first (ml/predict-probability model features)) 1.0))
        (is (= 780 (count (ml/coefficients model))) "every coefficient, sparse or not")
        (is (true? (ml/has-summary? model)))
        (is (= 1.0 (.accuracy (ml/evaluate libsvm model))) "a model's evaluate gives its summary")))
    (testing "trees"
      (let [small   (small-df)
            tree    (ml/fit small (ml/decision-tree-classifier {:max-depth 2}))
            boosted (ml/fit small (ml/gbt-classifier {:max-iter 2 :max-depth 2}))
            gbtr    (ml/fit small (ml/gbt-regressor {:max-iter 2 :max-depth 2}))]
        (is (double? (ml/predict-leaf tree [1.0 0.0 2.0])))
        (is (= 2 (count (ml/predict-leaf boosted [1.0 0.0 2.0]))))
        (is (includes? (ml/to-debug-string tree) "If (feature"))
        (is (= 2 (count (ml/evaluate-each-iteration small boosted))))
        (is (= 2 (count (ml/evaluate-each-iteration small gbtr "absolute"))))))
    (testing "clustering"
      (let [small     (small-df)
            k-means   (ml/fit small (ml/k-means {:k 2 :max-iter 2 :seed 1}))
            bisecting (ml/fit small (ml/bisecting-k-means {:k 2 :max-iter 2 :seed 1}))
            mixture   (ml/fit small (ml/gaussian-mixture {:k 2 :max-iter 2 :seed 1}))]
        (is (= (ml/predict k-means [1.0 0.0 2.0]) (ml/predict k-means [2.0 1.0 1.0])))
        (is (not= (ml/predict k-means [1.0 0.0 2.0]) (ml/predict k-means [8.0 6.0 1.0])))
        (is (double? (ml/compute-cost small bisecting)))
        (is (= 2 (count (ml/predict-probability mixture [1.0 0.0 2.0]))))))
    (testing "LDA, distributed"
      (let [model (ml/fit (small-df) (ml/lda {:k 2 :max-iter 2 :seed 1 :optimizer "em"}))]
        (is (= [3 2] [(count (ml/topics-matrix model)) (count (first (ml/topics-matrix model)))]))
        (is (double? (ml/log-prior model)))
        (is (double? (ml/training-log-likelihood model)))
        (is (false? (ml/distributed? (ml/to-local model))))
        (is (nil? (ml/get-checkpoint-files model))))))
  (let [df        (g/table->dataset @spark [[2.0 0.0 1.0] [1.0 0.0 0.0] [3.0 0.0 1.0] [0.5 0.0 0.0]] [:x :z :y])
        assembled (ml/transform df (ml/vector-assembler {:input-cols [:x :z :y] :output-col :features}))]
    (testing "feature models"
      (is (= 2 (count (ml/explained-variance (ml/fit assembled (ml/pca {:input-col :features :output-col :p :k 2}))))))
      (let [scaler (ml/fit assembled (ml/robust-scaler {:input-col :features :output-col :r}))]
        (is (= [[1.0 0.0 0.0] [1.5 0.0 1.0]] [(ml/median scaler) (ml/range scaler)])))
      (is (= [[##-Inf 0.0 ##Inf] [##-Inf 1.0 ##Inf]]
             (ml/get-splits-array (ml/bucketizer {:splits-array [[##-Inf 0.0 ##Inf] [##-Inf 1.0 ##Inf]]
                                                  :input-cols [:a :b] :output-cols [:c :d]}))))
      (is (= [##-Inf 0.0 ##Inf] (ml/get-splits (ml/bucketizer {:splits [##-Inf 0.0 ##Inf]}))))))
  (let [words (g/table->dataset @spark [[["a" "b" "c"]] [["a" "b"]] [["b" "c" "d"]]] [:words])]
    (testing "text features"
      (let [tf  (ml/transform words (ml/hashing-tf {:input-col :words :output-col :tf :num-features 16}))
            idf (ml/fit tf (ml/idf {:input-col :tf :output-col :idf}))]
        (is (= 16 (count (ml/doc-freq idf))))
        (is (= 8 (reduce + (ml/doc-freq idf))))
        (is (= 3 (ml/num-docs idf))))
      (let [model (ml/fit words (ml/word2vec {:input-col :words :output-col :v :vector-size 3 :min-count 1 :seed 1}))]
        (is (= ["word" "similarity"] (g/column-names (ml/find-synonyms model "a" 2))))
        (is (= 2 (g/count (ml/find-synonyms model [0.1 0.2 0.3] 2))))
        (is (= 4 (g/count (ml/get-vectors model)))))))
  (testing "Gaussian naive Bayes, factorisation machines, AFT, isotonic and ALS"
    (let [small (small-df)]
      (is (= [3 3] (map count (ml/sigma (ml/fit small (ml/naive-bayes {:model-type "gaussian"}))))))
      (let [fm (ml/fit small (ml/fm-classifier {:max-iter 2 :factor-size 2}))]
        (is (= [3 2 3] [(count (ml/factors fm)) (count (first (ml/factors fm))) (count (ml/linear fm))])))
      (is (= 0.0 (ml/predict (ml/fit small (ml/isotonic-regression {})) 1.0))))
    (let [aft (ml/fit (g/table->dataset @spark
                                        [[1.218 1.0 (g/dense [1.560 -0.605])]
                                         [2.949 0.0 (g/dense [0.346 2.158])]
                                         [3.627 0.0 (g/dense [1.380 0.231])]
                                         [0.273 1.0 (g/dense [0.520 1.151])]
                                         [4.199 0.0 (g/dense [0.795 -0.226])]]
                                        [:label :censor :features])
                      (ml/aft-survival-regression {:quantile-probabilities [0.3 0.6] :max-iter 5}))]
      (is (= 2 (count (ml/predict-quantiles aft [1.0 0.5])))))
    (is (= 3 (ml/rank (ml/fit (g/table->dataset @spark [[0 0 1.0] [0 1 2.0] [1 1 3.0]] [:user :item :rating])
                              (ml/als {:rank 3 :max-iter 2 :seed 1})))))))

(defn- regression-df
  "Six rows of a label and two features, for regressions."
  []
  (g/table->dataset @spark
                    (for [[y a b] [[1.0 1.0 2.0] [2.0 2.0 1.0] [3.0 3.0 3.0] [4.0 4.0 2.0] [5.0 5.0 6.0] [6.1 6.0 1.0]]]
                      [y (g/dense a b)])
                    [:label :features]))

(deftest ^:slow summaries-test
  (let [small (small-df)]
    (testing "a binary classifier's training summary, as a map"
      (let [model   (ml/fit small (ml/logistic-regression {:max-iter 5}))
            summary (ml/summary model)]
        (is (= [1.0 1.0 [0.0 1.0]] ((juxt :accuracy :area-under-roc :labels) summary)))
        (is (= 5 (:total-iterations summary)))
        (is (= 6 (count (:objective-history summary))))
        (is (every? #(instance? Dataset (% summary)) [:predictions :roc :pr :f-measure-by-threshold]))
        (is (= summary (ml/binary-summary model)))
        (testing "and one for new data, through evaluate, without the training's history"
          (let [evaluated (ml/summary (ml/evaluate small model))]
            (is (= 1.0 (:accuracy evaluated)))
            (is (not (contains? evaluated :objective-history)))))))
    (testing "a multilayer perceptron's, which isn't binary"
      (let [summary (ml/summary (ml/fit small (ml/mlp-classifier {:layers [3 2] :max-iter 5})))]
        (is (= [0.0 1.0] (:labels summary)))
        (is (not (contains? summary :area-under-roc)))))
    (testing "clustering models'"
      (is (= [[4 4] 2] ((juxt :cluster-sizes :k) (ml/summary (ml/fit small (ml/k-means {:k 2 :seed 1}))))))
      (let [summary (ml/summary (ml/fit small (ml/gaussian-mixture {:k 2 :seed 1})))]
        (is (double? (:log-likelihood summary)))
        (is (instance? Dataset (:probability summary)))))
    (testing "and an error for a model without one, or anything else"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"DecisionTreeClassificationModel has no summary"
                            (ml/summary (ml/fit small (ml/decision-tree-classifier {})))))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"not a java.lang.String"
                            (ml/summary "x")))))
  (let [regression (regression-df)]
    (testing "a linear regression's, with the coefficients' statistics from the normal solver"
      (let [summary (ml/summary (ml/fit regression (ml/linear-regression {:solver "normal"})))]
        (is (< 0.99 (:r2 summary) 1.0))
        (is (= 6 (:num-instances summary)))
        (is (= 3 (count (:p-values summary))))
        (is (instance? Dataset (:residuals summary)))))
    (testing "without them, which Spark can't give, from another solver"
      (is (not (contains? (ml/summary (ml/fit regression (ml/linear-regression {:solver "l-bfgs"
                                                                                :reg-param 0.1
                                                                                :elastic-net-param 0.5})))
                          :p-values))))
    (testing "a generalised linear regression's"
      (let [summary (ml/summary (ml/fit regression (ml/generalized-linear-regression {:max-iter 5})))]
        (is (= "irls" (:solver summary)))
        (is (= ["(Intercept)" "features_0" "features_1"]
               (map :feature (:coefficients-with-statistics summary))))
        (is (double? (:aic summary)))))))

(deftest ^:slow summarizer-test
  (let [stats (-> (small-df)
                  (g/with-column :weight (g/lit 2.0))
                  (g/agg {:plain    (ml/summarizer :features [:mean :count :num-non-zeros])
                          :weighted (ml/summarizer :features [:mean] :weight)})
                  g/collect
                  first)]
    (is (= [:count :mean :numNonZeros] (sort (keys (:plain stats)))))
    (is (= [8 [8.0 6.0 6.0]] ((juxt :count :numNonZeros) (:plain stats))))
    (doseq [means [(:mean (:plain stats)) (:mean (:weighted stats))]]
      (is (every? #(< (Math/abs (double %)) 1e-12) (map - means [4.75 3.125 1.25]))))))

(deftest instantiation-fpm-test
  (is (= (:max-pattern-length (ml/params (ml/prefix-span {:max-pattern-length 321}))) 321))
  (is (instance? PrefixSpan (ml/prefix-span {})))

  (is (= (:min-support (ml/params (ml/frequent-pattern-growth {:min-support 0.12345}))) 0.12345))
  (is (instance? FPGrowth (ml/fp-growth {}))))

(deftest instantiation-recommendation-test
  (is (= (:num-user-blocks (ml/params (ml/als {:num-user-blocks 12345}))) 12345))
  (is (instance? ALS (ml/alternating-least-squares {}))))

(deftest instantiation-clustering-test
  (is (= (:init-mode (ml/params (ml/power-iteration-clustering {:init-mode "degree"}))) "degree"))
  (is (instance? PowerIterationClustering (ml/power-iteration-clustering {})))

  (is (= (:features-col (ml/params (ml/gaussian-mixture {:features-col "fts"}))) "fts"))
  (is (instance? GaussianMixture (ml/gmm {})))

  (is (= (:distance-measure (ml/params (ml/bisecting-k-means {:distance-measure "cosine"}))) "cosine"))
  (is (instance? BisectingKMeans (ml/bisecting-k-means {})))

  (is (= (:optimizer (ml/params (ml/lda {:optimizer "em"}))) "em"))
  (is (instance? LDA (ml/latent-dirichlet-allocation {})))

  (is (= (:k (ml/params (ml/k-means {:k 123}))) 123))
  (is (instance? KMeans (ml/k-means {}))))

(deftest instantiation-evaluator-test
  (is (= (:k (ml/params (ml/ranking-evaluator {:k 12}))) 12))
  (is (instance? RankingEvaluator (ml/ranking-evaluator {})))

  (is (= (:label-col (ml/params (ml/multilabel-classification-evaluator {:label-col "xyz"}))) "xyz"))
  (is (instance? MultilabelClassificationEvaluator (ml/multilabel-classification-evaluator {})))

  (is (= (:raw-prediction-col (ml/params (ml/binary-classification-evaluator {:raw-prediction-col "xyz"}))) "xyz"))
  (is (instance? BinaryClassificationEvaluator (ml/binary-classification-evaluator {})))

  (is (= (:distance-measure (ml/params (ml/clustering-evaluator {:distance-measure "cosine"}))) "cosine"))
  (is (instance? ClusteringEvaluator (ml/clustering-evaluator {})))

  (is (= (:label-col (ml/params (ml/multiclass-classification-evaluator {:label-col "weightz"}))) "weightz"))
  (is (instance? MulticlassClassificationEvaluator (ml/multiclass-classification-evaluator {})))

  (is (= (:metric-name (ml/params (ml/regression-evaluator {:metric-name "r2"}))) "r2"))
  (is (instance? RegressionEvaluator (ml/regression-evaluator {}))))

(deftest instantiation-regression-test
  (is (= (:factor-size (ml/params (ml/fm-regressor {:factor-size 12}))) 12))
  (is (instance? FMRegressor (ml/fm-regressor {})))

  (is (= (:label-col (ml/params (ml/isotonic-regression {:label-col "ABC"}))) "ABC"))
  (is (instance? IsotonicRegression (ml/isotonic-regression {})))

  (is (= (:quantile-probabilities (ml/params (ml/aft-survival-regression {:quantile-probabilities [0.005 0.995]}))) [0.005 0.995]))
  (is (instance? AFTSurvivalRegression (ml/aft-survival-regression {})))

  (is (= (:max-bins (ml/params (ml/gbt-regressor {:max-bins 128}))) 128))
  (is (instance? GBTRegressor (ml/gbt-regressor {})))

  (is (= (:prediction-col (ml/params (ml/random-forest-regressor {:prediction-col "xyz"}))) "xyz"))
  (is (instance? RandomForestRegressor (ml/random-forest-regressor {})))

  (is (= (:variance-col (ml/params (ml/decision-tree-regressor {:variance-col "abc"}))) "abc"))
  (is (instance? DecisionTreeRegressor (ml/decision-tree-regressor {})))

  (is (= (:reg-param (ml/params (ml/glm {:reg-param 1.0}))) 1.0))
  (is (instance? GeneralizedLinearRegression (ml/generalized-linear-regression {})))

  (is (= (:standardization (ml/params (ml/linear-regression {:standardisation false}))) false))
  (is (instance? LinearRegression (ml/linear-regression {}))))

(deftest instantiation-classification-test
  (is (= (:init-std (ml/params (ml/fm-classifier {:init-std 10.0}))) 10.0))
  (is (instance? FMClassifier (ml/fm-classifier {})))

  (is (= (:thresholds (ml/params (ml/logistic-regression {:thresholds [0.0 0.1]}))) [0.0 0.1]))
  (is (instance? LogisticRegression (ml/logistic-regression {})))

  (is (= (:thresholds (ml/params (ml/naive-bayes {:thresholds [0.0 0.1]}))) [0.0 0.1]))
  (is (instance? NaiveBayes (ml/naive-bayes {})))

  (let [classifier (ml/logistic-regression {:max-iter 10 :tol 1e-6})]
    (is (instance? LogisticRegression (:classifier (ml/params (ml/one-vs-rest {:classifier classifier}))))))
  (is (instance? OneVsRest (ml/one-vs-rest {})))

  (is (= (:standardization (ml/params (ml/linear-svc {:standardisation false}))) false))
  (is (instance? LinearSVC (ml/linear-svc {})))

  (is (= (:layers (ml/params (ml/mlp-classifier {:layers [1 2 3]}))) [1 2 3]))
  (is (instance? MultilayerPerceptronClassifier (ml/mlp-classifier {})))

  (is (= (:feature-subset-strategy (ml/params (ml/gbt-classifier {:feature-subset-strategy "auto"}))) "auto"))
  (is (instance? GBTClassifier (ml/gbt-classifier {})))

  (is (= (:num-trees (ml/params (ml/random-forest-classifier {:num-trees 12}))) 12))
  (is (instance? RandomForestClassifier (ml/random-forest-classifier {})))

  (is (= (:thresholds (ml/params (ml/decision-tree-classifier {:thresholds [0.0]}))) [0.0]))
  (is (instance? DecisionTreeClassifier (ml/decision-tree-classifier {}))))

(deftest instantiation-features-test
  (is (= (:with-centering (ml/params (ml/robust-scaler {:with-centering true}))) true))
  (is (instance? RobustScaler (ml/robust-scaler {})))

  (is (= (:case-sensitive (ml/params (ml/stop-words-remover {:case-sensitive true}))) true))
  (is (instance? StopWordsRemover (ml/stop-words-remover {})))
  (is (= 181 (-> (ml/stop-words-remover {}) ml/params :stop-words count)))

  (is (= (:num-top-features (ml/params (ml/chi-sq-selector {:num-top-features 1122}))) 1122))
  (is (instance? ChiSqSelector (ml/chi-sq-selector {})))

  (is (= (:handle-invalid (ml/params (ml/vector-assembler {:handle-invalid "skip"}))) "skip"))
  (is (instance? VectorAssembler (ml/vector-assembler {})))

  (is (= (:input-cols (ml/params (ml/feature-hasher {:input-cols ["real" "bool" "stringNum" "string"]}))) ["real" "bool" "stringNum" "string"]))
  (is (instance? FeatureHasher (ml/feature-hasher {})))

  (is (= (:input-col (ml/params (ml/n-gram {:input-col "words"}))) "words"))
  (is (instance? NGram (ml/n-gram {})))

  (is (= (:threshold (ml/params (ml/binariser {:threshold 0.5}))) 0.5))
  (is (instance? Binarizer (ml/binarizer {})))

  (is (= (:k (ml/params (ml/pca {:k 3}))) 3))
  (is (instance? PCA (ml/pca {})))

  (is (= (:degree (ml/params (ml/polynomial-expansion {:degree 3}))) 3))
  (is (instance? PolynomialExpansion (ml/polynomial-expansion {})))

  (is (= (:inverse (ml/params (ml/dct {:inverse true}))) true))
  (is (instance? DCT (ml/discrete-cosine-transform {})))

  (is (= (:handle-invalid (ml/params (ml/string-indexer {:handle-invalid "skip"}))) "skip"))
  (is (instance? StringIndexer (ml/string-indexer {})))

  (is (= (:output-col (ml/params (ml/index-to-string {:output-col "categoryIndex"}))) "categoryIndex"))
  (is (instance? IndexToString (ml/index-to-string {})))

  (is (= (:input-cols (ml/params (ml/one-hot-encoder {:input-cols ["categoryIndex1" "categoryIndex2"]}))) ["categoryIndex1" "categoryIndex2"]))
  (is (instance? OneHotEncoder (ml/one-hot-encoder {})))

  (is (= (:max-categories (ml/params (ml/vector-indexer {:max-categories 10}))) 10))
  (is (instance? VectorIndexer (ml/vector-indexer {})))

  (is (= (:output-col (ml/params (ml/interaction {:output-col "indexed"}))) "indexed"))
  (is (instance? Interaction (ml/interaction {})))

  (is (= (:p (ml/params (ml/normaliser {:p 1.0}))) 1.0))
  (is (instance? Normalizer (ml/normalizer {})))

  (is (= (:input-col (ml/params (ml/standard-scaler {:input-col "abcdef"}))) "abcdef"))
  (is (instance? StandardScaler (ml/standard-scaler {})))

  (is (= (:min (ml/params (ml/min-max-scaler {:min -9999}))) -9999.0))
  (is (instance? MinMaxScaler (ml/min-max-scaler {})))

  (is (= (:output-col (ml/params (ml/max-abs-scaler {:output-col "xyz"}))) "xyz"))
  (is (instance? MaxAbsScaler (ml/max-abs-scaler {})))

  (is (= (:splits (ml/params (ml/bucketiser {:splits [-999.9 -0.5 -0.3 0.0 0.2 999.9]}))) [-999.9 -0.5 -0.3 0.0 0.2 999.9]))
  (is (instance? Bucketizer (ml/bucketiser {})))

  (is (= (:scaling-vec (ml/params (ml/elementwise-product {:scaling-vec [0.0 1.0 2.0]}))) [0.0 1.0 2.0]))
  (is (instance? ElementwiseProduct (ml/elementwise-product {})))

  (is (= (:statement (ml/params (ml/sql-transformer {:statement "SELECT *, (v1 + v2)"}))) "SELECT *, (v1 + v2)"))
  (is (instance? SQLTransformer (ml/sql-transformer {})))

  (is (= (:size (ml/params (ml/vector-size-hint {:size 3}))) 3))
  (is (instance? VectorSizeHint (ml/vector-size-hint {})))

  (is (= (:num-buckets (ml/params (ml/quantile-discretiser {:num-buckets 3}))) 3))
  (is (instance? QuantileDiscretizer (ml/quantile-discretizer {})))

  (is (= (:input-cols (ml/params (ml/imputer {:input-cols ["a" "b"]}))) ["a" "b"]))
  (is (instance? Imputer (ml/imputer {})))

  (is (= (:bucket-length (ml/params (ml/bucketed-random-projection-lsh {:bucket-length 2.0}))) 2.0))
  (is (instance? BucketedRandomProjectionLSH (ml/bucketed-random-projection-lsh {})))

  (is (= (:num-hash-tables (ml/params (ml/min-hash-lsh {:num-hash-tables 55}))) 55))
  (is (instance? MinHashLSH (ml/min-hash-lsh {})))

  (let [actual (ml/params (ml/count-vectoriser {:min-df 2.0 :min-tf 3.0 :max-df 4.0}))]
    (is (and (= (:min-df actual) 2.0)
             (= (:min-tf actual) 3.0)
             (= (:max-df actual) 4.0))))
  (is (instance? CountVectorizer (ml/count-vectorizer {})))

  (is (= (:min-doc-freq (ml/params (ml/idf {:min-doc-freq 100}))) 100))
  (is (instance? IDF (ml/idf {})))

  (is (= (:input-col (ml/params (ml/tokeniser {:input-col "sentence"}))) "sentence"))
  (is (instance? Tokenizer (ml/tokenizer {})))

  (is (= (:output-col (ml/params (ml/hashing-tf {:output-col "rawFeatures"}))) "rawFeatures"))
  (is (instance? HashingTF (ml/hashing-tf {})))

  (is (= (:vector-size (ml/params (ml/word2vec {:vector-size 3}))) 3))
  (is (instance? Word2Vec (ml/word2vec {})))

  (is (= (:pattern (ml/params (ml/regex-tokeniser {:pattern "\\W"}))) "\\W"))
  (is (instance? RegexTokenizer (ml/regex-tokenizer {})))

  (is (= "y ~ ." (:formula (ml/params (ml/r-formula {:formula "y ~ ."})))))
  (is (instance? RFormula (ml/r-formula {})))

  (is (= 3.0 (:selection-threshold (ml/params (ml/univariate-feature-selector {:selection-threshold 3})))))
  (is (instance? UnivariateFeatureSelector (ml/univariate-feature-selector {})))

  (is (= 0.5 (:variance-threshold (ml/params (ml/variance-threshold-selector {:variance-threshold 0.5})))))
  (is (instance? VarianceThresholdSelector (ml/variance-threshold-selector {})))

  (is (= [1 2] (:indices (ml/params (ml/vector-slicer {:indices [1 2]})))))
  (is (instance? VectorSlicer (ml/vector-slicer {})))

  (if (spark-4?)
    (is (= "binary" (:target-type (ml/params (ml/target-encoder {:target-type "binary"})))))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"ml/target-encoder needs Spark 4\.0"
                          (ml/target-encoder {})))))

(deftest ^:slow pipeline-test
  (testing "should be able to fit the example stages"
    (let [dataset     (g/table->dataset
                       @spark
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
      (is (includes? (:probability dtypes) "Vector"))
      (is (= "DoubleType" (:prediction dtypes)))))
  (testing "should be able to fit the idf example"
    (let [dataset     (g/table->dataset
                       @spark
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
      (let [features (map first (g/collect-vals transformed))]
        (is (= 3 (count features)))
        (is (every? #(and (seq (:values %)) (every? double? (:values %))) features)))
      (is (every? double? (-> transformer ml/stages last ml/idf-vector)))))
  (testing "should be able to fit the word2vec example"
    (let [dataset     (g/table->dataset
                       @spark
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
      (is (every? double? (->> transformed g/collect-vals flatten))))))

(deftest hypothesis-testing-test
  (let [dataset (g/table->dataset
                 @spark
                 [[0.0 (g/dense 0.5 10.0)]
                  [0.0 (g/dense 1.5 20.0)]
                  [1.0 (g/dense 1.5 30.0)]
                  [0.0 (g/dense 3.5 30.0)]
                  [0.0 (g/dense 3.5 40.0)]
                  [1.0 (g/dense 3.5 40.0)]]
                 [:label :features])]
    (testing "able to do chi-squared test"
      (is (every? double? (-> dataset
                              (ml/chi-square-test "features" "label")
                              g/first-vals
                              first))))
    (testing "and with a row per feature"
      (is (= [0 1] (-> dataset
                       (ml/chi-square-test "features" "label" true)
                       (g/order-by :featureIndex)
                       (g/collect-col :featureIndex)))))
    (testing "able to do KS test"
      (let [actual (-> (df-20)
                       (ml/kolmogorov-smirnov-test :Rooms "norm" [2.35 0.745])
                       g/first-vals)]
        (is (and (< 0.01 (first actual) 0.1)
                 (< 0.25 (second actual) 0.35)))))))

(deftest ^:slow correlation-test
  (let [dataset     (g/table->dataset
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
    (testing "should be able to make vectors"
      (is (= [1.0 0.0 -2.0 0.0] (-> features-df g/first-vals first))))
    (testing "should be able to calculate correlation"
      (let [corr-matrix (-> features-df
                            (g/corr "features")
                            g/first-vals
                            first)]
        (is (= 4 (count corr-matrix)))
        (is (= 4 (count (first corr-matrix))))
        (is (every? double? (flatten corr-matrix)))))
    (testing "should be able to calculate correlation"
      (is (includes? (-> features-df
                         (g/with-column :features-array (ml/vector->array :features))
                         g/dtypes
                         :features-array) "ArrayType")))))

(deftest param-extraction-test
  (is (= {:max-iter 100,
          :family "auto",
          :tol 1.0E-6,
          :raw-prediction-col "rawPrediction",
          :elastic-net-param 0.0,
          :reg-param 0.0,
          :aggregation-depth 2,
          :threshold 0.5,
          :fit-intercept true,
          :label-col "label",
          :max-block-size-in-mb 0.0
          :standardization true,
          :probability-col "probability",
          :prediction-col "prediction",
          :features-col "features"}
         (ml/params (ml/logistic-regression {})))))
