(ns ^:classic zero-one.geni.ml-test
  (:require
   [clojure.string :refer [includes?]]
   [clojure.test :refer [deftest is testing]]
   [zero-one.geni.core :as g]
   [zero-one.geni.interop :as interop]
   [zero-one.geni.ml :as ml]
   [zero-one.geni.utils :refer [class-named]]
   [zero-one.geni.test-resources :refer [create-temp-file!
                                         spark-at-least?
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
   (org.apache.spark.ml.tuning CrossValidator
                               TrainValidationSplit)
   (org.apache.spark.sql Dataset)))

(deftest reading-and-writing-test
  (let [stage     (ml/vector-assembler {})
        temp-file (.toString (create-temp-file! ".xml"))]
    (is (nil? (ml/write-stage! stage temp-file {:mode "overwrite"})))
    (is (thrown? Exception (ml/write-stage! stage temp-file)))
    (is (nil? (ml/write-stage! stage temp-file {:mode :overwrite})))
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
  (testing "keywords and strings mix in a collection"
    (is (= ["a" "b"] (ml/input-cols (ml/vector-assembler {:input-cols [:a "b"]})))))
  (testing "a setter that takes an MLlib vector takes numbers"
    (is (= [-1.0] (:lower-bounds-on-intercepts
                   (ml/params (ml/logistic-regression {:lower-bounds-on-intercepts [-1]})))))
    (is (= [0.0 2.0] (:scaling-vec (ml/params (ml/elementwise-product {:scaling-vec [0 2]}))))))
  (testing "the British spelling of standardisation still works"
    (is (false? (.getStandardization (ml/logistic-regression {:standardisation false}))))
    (is (false? (.getStandardization (ml/linear-regression {:standardisation false}))))))

(deftest spark-defaults-test
  (testing "Geni sets only the params given, so Spark's defaults hold"
    (is (= 0.0 (:threshold (ml/params (ml/binarizer {})))))
    (is (= (.getSeed (MinHashLSH.)) (.getSeed (ml/min-hash-lsh {}))))
    (let [glm (ml/glm {})]
      (is (not (.isSet glm (.variancePower glm))) "so a gaussian fit logs no warning")))
  (let [df (g/table->dataset @spark [[0.2 3.0] [0.8 1.0] [0.6 2.0]] [:a :b])]
    (testing "a binariser of several columns takes :thresholds"
      (is (= [[0.0 1.0] [1.0 0.0] [1.0 1.0]]
             (-> df
                 (ml/transform (ml/binarizer {:input-cols [:a :b] :output-cols [:c :d] :thresholds [0.5 1.5]}))
                 (g/select :c :d)
                 g/collect-vals))))
    (testing "and a quantile discretiser of several columns :num-buckets-array"
      (is (= 2 (count (ml/get-splits-array
                       (ml/fit df (ml/quantile-discretizer {:input-cols [:a :b] :output-cols [:c :d]
                                                            :num-buckets-array [2 3]})))))))))

(deftest stage-test
  (testing "a stage from its class, its name as a string or a symbol, with Geni's params"
    (doseq [cls [Tokenizer
                 "org.apache.spark.ml.feature.Tokenizer"
                 'org.apache.spark.ml.feature.Tokenizer]]
      (let [stage (ml/stage cls {:input-col :text :output-col :words})]
        (is (instance? Tokenizer stage))
        (is (= {:input-col "text" :output-col "words"} (ml/params stage))))))
  (testing "a stage made already gets its params in place"
    (let [tokenizer (Tokenizer.)]
      (is (identical? tokenizer (ml/stage tokenizer {:input-col :text})))
      (is (= "text" (.getInputCol tokenizer)))))
  (testing "a class that isn't on the classpath, a param the class lacks, and a value
            that isn't a stage throw"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"There's no class com\.example\.NoSuchStage on the classpath\."
                          (ml/stage "com.example.NoSuchStage" {})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Tokenizer has no param :input-cols\. Did you mean :input-col\?"
                          (ml/stage Tokenizer {:input-cols [:text]})))
    (is (thrown-with-msg? IllegalArgumentException
                          #"ml/stage takes a class, a class name or a stage, not :tokenizer\."
                          (ml/stage :tokenizer {}))))
  (testing "its stages go into a pipeline"
    (let [dataset  (g/table->dataset @spark [["Spark and Geni"]] [:text])
          pipeline (ml/pipeline
                    (ml/stage Tokenizer {:input-col :text :output-col :words})
                    (ml/stage "org.apache.spark.ml.feature.StopWordsRemover"
                              {:input-col :words :output-col :kept}))]
      (is (= [{:kept ["spark" "geni"]}]
             (-> dataset
                 (ml/transform (ml/fit dataset pipeline))
                 (g/select :kept)
                 g/collect))))))

(deftest stages-without-a-session-test
  (testing "write-stage! and read-stage! use Geni's default session, rather than Spark's
            getOrCreate, which needs a master URL"
    (let [temp-file (.toString (create-temp-file! ".stage"))
          stage     (ml/vector-assembler {:input-cols [:a :b] :output-col :v})]
      (stop-session!)
      (ml/write-stage! stage temp-file {:mode "overwrite"})
      (stop-session!)
      (is (= ["a" "b"] (seq (.getInputCols (ml/read-stage! VectorAssembler temp-file))))))))

(deftest feature-extraction-test
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

(deftest clustering-test
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

(deftest multinomial-classification-test
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

(deftest binary-classification-test
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

(deftest decision-tree-classifier-test
  (let [estimator   (ml/decision-tree-classifier {})
        model       (ml/fit (libsvm-df) estimator)]
    (testing "Attributes are callable"
      (is (= 2 (ml/depth model)))
      (is (= 5 (ml/num-nodes model)))
      (is (not (nil? (ml/root-node model)))))))

(deftest random-forest-classifier-test
  (let [estimator   (ml/random-forest-classifier {:num-trees 2 :max-depth 2})
        model       (ml/fit (libsvm-df) estimator)]
    (testing "Attributes are callable"
      (is (= 780 (count (ml/feature-importances model))) "every feature's, zero or not")
      (is (every? double? (ml/feature-importances model)))
      (is (int? (ml/total-num-nodes model)))
      (is (seq? (ml/trees model))))))

(deftest gradient-boosted-tree-classifier-test
  (let [estimator   (ml/gbt-classifier {:max-iter 2 :max-depth 2})
        model       (ml/fit (libsvm-df) estimator)]
    (testing "Attributes are callable"
      (is (= 780 (count (ml/feature-importances model))) "every feature's, zero or not")
      (is (every? double? (ml/feature-importances model)))
      (is (int? (ml/total-num-nodes model)))
      (is (seq? (ml/trees model)))
      (is (int? (ml/get-num-trees model)))
      (is (every? double? (ml/tree-weights model))))))

(deftest naive-bayes-classifier-test
  (let [estimator   (ml/naive-bayes {})
        model       (ml/fit (libsvm-df) estimator)]
    (testing "Attributes are callable"
      (let [actual (ml/theta model)]
        (is (and (every? seq? actual)
                 (every? double? (flatten actual)))))
      (is (every? double? (ml/pi model))))))

(deftest isotonic-regressor-test
  (let [estimator   (ml/isotonic-regression {})
        model       (ml/fit (libsvm-df) estimator)]
    (testing "Attributes are callable"
      (is (every? double? (ml/boundaries model))))))

(deftest aft-survival-regression-test
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

(deftest k-means-clustering-test
  (let [estimator   (ml/k-means {:max-iter 2})
        model       (ml/fit (k-means-df) estimator)]
    (testing "Attributes are callable"
      (let [actual (ml/cluster-centers model)]
        (is (and (every? seq? actual)
                 (every? double? (flatten actual))))))))

(deftest lda-clustering-test
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

(deftest gmm-clustering-test
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

(deftest feature-models-test
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
      (when (spark-at-least? "4.0")
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
    (is (= ["1" "2"] (first (ml/labels-array (ml/string-indexer-model {:labels [1 2] :input-col :n})))))
    (is (= [1.0 0.0 1.0]
           (-> (g/table->dataset @spark [[2] [1] [2]] [:n])
               (ml/transform (ml/string-indexer-model {:labels [1 2] :input-col :n :output-col :i}))
               (g/collect-col :i))))
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

(deftest model-accessors-test
  (let [features (first-features)
        libsvm   (g/limit (libsvm-df) 60)]
    (testing "one row's predictions"
      (let [model (ml/fit libsvm (ml/logistic-regression {:max-iter 5}))]
        (is (= 0.0 (ml/predict model features)))
        (is (= 1.0 (ml/predict model (vec (repeat 780 0.0)))))
        (is (= 2 (count (ml/predict-raw model features))))
        (is (< 0.99 (first (ml/predict-probability model features)) 1.0))
        (testing "from a sparse vector's map, as g/collect gives it"
          (let [collected (-> libsvm (g/select :features) g/first-vals first)]
            (is (= [:size :indices :values] (keys collected)))
            (is (= (ml/predict-raw model features) (ml/predict-raw model collected)))))
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
        (is (= 3 (ml/num-docs idf)))
        (is (= 16 (count (ml/idf-vector idf)))))
      (let [model (ml/fit words (ml/word2vec {:input-col :words :output-col :v :vector-size 3 :min-count 1 :seed 1}))]
        (is (= ["word" "similarity"] (g/column-names (ml/find-synonyms model "a" 2))))
        (is (= 2 (g/count (ml/find-synonyms model [0.1 0.2 0.3] 2))))
        (is (= (g/collect (ml/find-synonyms model [0.1 0.2 0.3] 2))
               (g/collect (ml/find-synonyms model (g/dense 0.1 0.2 0.3) 2))))
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

(deftest summaries-test
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
    (testing "without the p-values, which Spark can't give for more features than rows"
      (let [wide    (g/table->dataset @spark
                                      (for [[y a b c d] [[1.0 1.0 2.0 0.0 1.0] [2.0 2.0 1.0 1.0 0.0] [3.0 3.0 3.0 1.0 1.0]]]
                                        [y (g/dense a b c d)])
                                      [:label :features])
            summary (ml/summary (ml/fit wide (ml/linear-regression {:solver "normal" :reg-param 0.1})))]
        (is (double? (:r2 summary)))
        (is (= 3 (:num-instances summary)))
        (is (not (contains? summary :p-values)))))
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

(deftest clusters-correlation-and-evaluators-test
  (testing "power iteration clustering's clusters, which it assigns rather than fits"
    (let [edges    (g/table->dataset @spark
                                     [[0 1 1.0] [1 2 1.0] [0 2 1.0] [3 4 1.0] [4 5 1.0] [3 5 1.0] [2 3 0.01]]
                                     [:src :dst :weight])
          clusters (->> (ml/assign-clusters edges (ml/power-iteration-clustering {:k 2 :weight-col "weight"
                                                                                  :max-iter 10}))
                        g/collect
                        (map (juxt :id :cluster))
                        (into (sorted-map)))]
      (is (= (range 6) (keys clusters)))
      (is (apply = (map clusters [0 1 2])))
      (is (apply = (map clusters [3 4 5])))
      (is (not= (clusters 0) (clusters 3)))))
  (testing "a vector column's correlation matrix, by Spearman's rank too"
    (let [ranks   (g/table->dataset @spark (for [[a b] [[1.0 1.0] [2.0 4.0] [3.0 9.0] [4.0 100.0]]]
                                             [(g/dense a b)])
                                    [:features])
          by      #(-> ranks (ml/correlation :features %) g/first-vals first)
          pearson (by "pearson")
          matrix  (by "spearman")]
      (is (= ["spearman(features)"] (g/column-names (ml/correlation ranks :features "spearman"))))
      (is (= [2 2] [(count matrix) (count (first matrix))]))
      (is (< (Math/abs (- 1.0 (-> matrix first second))) 1e-12) "the same ranks")
      (is (< (-> pearson first second) 0.9) "where Pearson's is lower")))
  (testing "whether an evaluator's larger metric is better"
    (is (true? (ml/larger-better? (ml/binary-classification-evaluator {}))))
    (is (false? (ml/larger-better? (ml/regression-evaluator {:metric-name "rmse"}))))))

(deftest summarizer-test
  (let [stats (-> (small-df)
                  ;; 1 for label 0 and 3 for label 1.
                  (g/with-column :weight (g/+ 1.0 (g/* 2.0 :label)))
                  (g/agg {:plain    (ml/summarizer :features [:mean :count :num-non-zeros])
                          :weighted (ml/summarizer :features [:mean] :weight)})
                  g/collect
                  first)]
    (is (= [:count :mean :numNonZeros] (sort (keys (:plain stats)))))
    (is (= [8 [8.0 6.0 6.0]] ((juxt :count :numNonZeros) (:plain stats))))
    (doseq [[means expected] [[(:mean (:plain stats)) [4.75 3.125 1.25]]
                              [(:mean (:weighted stats)) [6.375 4.4375 0.875]]]]
      (is (every? #(< (Math/abs (double %)) 1e-12) (map - means expected))))))

(def ^:private constructors
  "Each constructor, or an alias of it, its class, and params for it to set,
  with what `ml/params` gives back for them where that differs."
  [[ml/prefix-span PrefixSpan {:max-pattern-length 321}]
   [ml/frequent-pattern-growth FPGrowth {:min-support 0.12345}]
   [ml/alternating-least-squares ALS {:num-user-blocks 12345}]
   [ml/power-iteration-clustering PowerIterationClustering {:init-mode "degree"}]
   [ml/gmm GaussianMixture {:features-col "fts"}]
   [ml/bisecting-k-means BisectingKMeans {:distance-measure "cosine"}]
   [ml/latent-dirichlet-allocation LDA {:optimizer "em"}]
   [ml/k-means KMeans {:k 123}]
   [ml/ranking-evaluator RankingEvaluator {:k 12}]
   [ml/multilabel-classification-evaluator MultilabelClassificationEvaluator {:label-col "xyz"}]
   [ml/binary-classification-evaluator BinaryClassificationEvaluator {:raw-prediction-col "xyz"}]
   [ml/clustering-evaluator ClusteringEvaluator {:distance-measure "cosine"}]
   [ml/multiclass-classification-evaluator MulticlassClassificationEvaluator {:label-col "weightz"}]
   [ml/regression-evaluator RegressionEvaluator {:metric-name "r2"}]
   [ml/fm-regressor FMRegressor {:factor-size 12}]
   [ml/isotonic-regression IsotonicRegression {:label-col "ABC"}]
   [ml/aft-survival-regression AFTSurvivalRegression {:quantile-probabilities [0.005 0.995]}]
   [ml/gbt-regressor GBTRegressor {:max-bins 128}]
   [ml/random-forest-regressor RandomForestRegressor {:prediction-col "xyz"}]
   [ml/decision-tree-regressor DecisionTreeRegressor {:variance-col "abc"}]
   [ml/glm GeneralizedLinearRegression {:reg-param 1.0}]
   [ml/linear-regression LinearRegression {:standardisation false} {:standardization false}]
   [ml/fm-classifier FMClassifier {:init-std 10.0}]
   [ml/logistic-regression LogisticRegression {:thresholds [0.0 0.1]}]
   [ml/naive-bayes NaiveBayes {:thresholds [0.0 0.1]}]
   [ml/one-vs-rest OneVsRest {:classifier (ml/logistic-regression {:max-iter 10})}]
   [ml/linear-svc LinearSVC {:standardisation false} {:standardization false}]
   [ml/mlp-classifier MultilayerPerceptronClassifier {:layers [1 2 3]}]
   [ml/gbt-classifier GBTClassifier {:feature-subset-strategy "auto"}]
   [ml/random-forest-classifier RandomForestClassifier {:num-trees 12}]
   [ml/decision-tree-classifier DecisionTreeClassifier {:thresholds [0.0]}]
   [ml/robust-scaler RobustScaler {:with-centering true}]
   [ml/stop-words-remover StopWordsRemover {:case-sensitive true}]
   [ml/chi-sq-selector ChiSqSelector {:num-top-features 1122}]
   [ml/vector-assembler VectorAssembler {:handle-invalid "skip"}]
   [ml/feature-hasher FeatureHasher {:input-cols ["real" "bool" "stringNum" "string"]}]
   [ml/n-gram NGram {:input-col "words"}]
   [ml/binariser Binarizer {:threshold 0.5}]
   [ml/pca PCA {:k 3}]
   [ml/polynomial-expansion PolynomialExpansion {:degree 3}]
   [ml/discrete-cosine-transform DCT {:inverse true}]
   [ml/string-indexer StringIndexer {:handle-invalid "skip"}]
   [ml/index-to-string IndexToString {:output-col "categoryIndex"}]
   [ml/one-hot-encoder OneHotEncoder {:input-cols ["categoryIndex1" "categoryIndex2"]}]
   [ml/vector-indexer VectorIndexer {:max-categories 10}]
   [ml/interaction Interaction {:output-col "indexed"}]
   [ml/normaliser Normalizer {:p 1.0}]
   [ml/standard-scaler StandardScaler {:input-col "abcdef"}]
   [ml/min-max-scaler MinMaxScaler {:min -9999} {:min -9999.0}]
   [ml/max-abs-scaler MaxAbsScaler {:output-col "xyz"}]
   [ml/bucketiser Bucketizer {:splits [-999.9 -0.5 -0.3 0.0 0.2 999.9]}]
   [ml/elementwise-product ElementwiseProduct {:scaling-vec [0.0 1.0 2.0]}]
   [ml/sql-transformer SQLTransformer {:statement "SELECT *, (v1 + v2)"}]
   [ml/vector-size-hint VectorSizeHint {:size 3}]
   [ml/quantile-discretiser QuantileDiscretizer {:num-buckets 3}]
   [ml/imputer Imputer {:input-cols ["a" "b"]}]
   [ml/bucketed-random-projection-lsh BucketedRandomProjectionLSH {:bucket-length 2.0}]
   [ml/min-hash-lsh MinHashLSH {:num-hash-tables 55}]
   [ml/count-vectoriser CountVectorizer {:min-df 2.0 :min-tf 3.0 :max-df 4.0}]
   [ml/idf IDF {:min-doc-freq 100}]
   [ml/tokeniser Tokenizer {:input-col "sentence"}]
   [ml/hashing-tf HashingTF {:output-col "rawFeatures"}]
   [ml/word2vec Word2Vec {:vector-size 3}]
   [ml/regex-tokeniser RegexTokenizer {:pattern "\\W"}]
   [ml/r-formula RFormula {:formula "y ~ ."}]
   [ml/univariate-feature-selector UnivariateFeatureSelector {:selection-threshold 3} {:selection-threshold 3.0}]
   [ml/variance-threshold-selector VarianceThresholdSelector {:variance-threshold 0.5}]
   [ml/vector-slicer VectorSlicer {:indices [1 2]}]
   [ml/cross-validator CrossValidator {:num-folds 3}]
   [ml/train-validation-split TrainValidationSplit {:train-ratio 0.6}]])

(deftest constructors-test
  (doseq [[make cls params expected] constructors
          :let [expected (or expected params)]]
    (testing (.getSimpleName ^Class cls)
      (let [stage (make {})]
        (is (instance? cls stage))
        (is (not-any? #(.isSet stage %) (.params stage)) "no params set, so Spark's defaults hold"))
      (is (= expected (select-keys (ml/params (make params)) (keys expected))))))
  (testing "Spark's stop words, by default"
    (is (= 181 (-> (ml/stop-words-remover {}) ml/params :stop-words count))))
  (testing "target-encoder, which needs Spark 4"
    (if (spark-at-least? "4.0")
      (is (= "binary" (:target-type (ml/params (ml/target-encoder {:target-type "binary"})))))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"ml/target-encoder needs Spark 4\.0"
                            (ml/target-encoder {}))))))

(deftest xgboost-missing-test
  (when-not (class-named "ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier")
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"XGBoost4J-Spark 3 isn't on the classpath"
                          (ml/xgboost-classifier {})))))

(deftest pipeline-test
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
      (is (= "DoubleType" (:prediction dtypes))))))

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

(deftest correlation-test
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
