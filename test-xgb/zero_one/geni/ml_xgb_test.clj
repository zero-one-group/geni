(ns ^:classic ^:xgb zero-one.geni.ml-xgb-test
  "XGBoost4J-Spark's estimators, with XGBoost4J-Spark 3 on the classpath:
  clojure -T:build xgb-tests."
  (:require
   [clojure.java.io :as io]
   [clojure.test :refer [deftest is testing]]
   [zero-one.geni.core :as g]
   [zero-one.geni.ml :as ml]
   [zero-one.geni.test-resources :refer [create-temp-dir! libsvm-df spark-at-least?]])
  (:import
   (clojure.lang ExceptionInfo)
   (ml.dmlc.xgboost4j.scala.spark XGBoostClassificationModel
                                  XGBoostClassifier
                                  XGBoostRankerModel
                                  XGBoostRegressionModel
                                  XGBoostRegressor)))

(def ^:private small
  "Few rounds of shallow trees, which keep the tests fast."
  {:num-round 2 :max-depth 2})

(defn- training-df
  "The LIBSVM data with its features as arrays, since XGBoost4J-Spark 3.4.0
  predicts wrongly on sparse vectors."
  []
  (g/with-column (libsvm-df) :features (ml/vector-to-array :features)))

(defn- temp-path [dir-name]
  (str (io/file (create-temp-dir!) dir-name)))

(defn- accuracy [predictions]
  (let [pairs (g/collect-vals (g/select predictions :label :prediction))]
    (/ (count (filter (fn [[label prediction]] (== label prediction)) pairs))
       (count pairs))))

(deftest params-test
  (testing "the estimators have XGBoost's own defaults"
    (let [params (ml/params (ml/xgboost-classifier {}))]
      (is (= 100 (:num-round params)))
      (is (= 256 (:max-bin params)))
      (is (NaN? (:missing params)))))
  (testing "params go through, with :max-bin for :max-bins"
    (let [params (ml/params (ml/xgboost-regressor {:num-round 890 :max-bin 222}))]
      (is (= [890 222] [(:num-round params) (:max-bin params)])))
    (is (= 0.1 (:eta (ml/params (ml/xgboost-classifier {:eta 0.1})))))
    (is (= 0.0 (:missing (ml/params (ml/xgboost-classifier {:missing 0.0}))))))
  (testing ":features-col takes a column, or several"
    (is (= "features" (:features-col (ml/params (ml/xgboost-classifier {:features-col "features"})))))
    (is (= ["a" "b"] (:features-cols (ml/params (ml/xgboost-classifier {:features-col ["a" "b"]}))))))
  (testing "a param that XGBoost4J-Spark 3 dropped throws"
    (is (thrown-with-msg? ExceptionInfo #"has no param :cache-training-set"
                          (ml/xgboost-classifier {:cache-training-set true}))))
  (is (instance? XGBoostClassifier (ml/xgboost-classifier {})))
  (is (instance? XGBoostRegressor (ml/xgboost-regressor {}))))

(defn- round-trips?
  "Whether the model writes as a Spark ML stage and reads back with the same
  predictions."
  [model model-class dir-name]
  (let [path (temp-path dir-name)]
    (ml/write-stage! model path)
    (= (g/collect-col (ml/transform (training-df) model) :prediction)
       (g/collect-col (ml/transform (training-df) (ml/read-stage! model-class path)) :prediction))))

(deftest classifier-test
  (let [model       (ml/fit (training-df) (ml/xgboost-classifier small))
        predictions (ml/transform (training-df) model)]
    (is (instance? XGBoostClassificationModel model))
    (testing "fits the training data"
      (is (<= 0.9 (accuracy predictions))))
    ;; XGBoost4J-Spark 3.4.0 is built against Spark 3.5's json4s, so it can't
    ;; write a stage on Spark 4.
    (testing "saves and loads as a Spark ML stage, except on Spark 4"
      (if (spark-at-least? "4.0")
        (is (thrown? NoSuchMethodError (ml/write-stage! model (temp-path "classifier")))
            "XGBoost4J-Spark now saves stages on Spark 4: update the guide and the changelog")
        (is (round-trips? model XGBoostClassificationModel "classifier"))))
    (testing "saves its native booster"
      (let [path (temp-path "booster.json")]
        (ml/write-native-model! model path)
        (is (pos? (.length (io/file path))))))))

(deftest regressor-test
  (let [model       (ml/fit (training-df) (ml/xgboost-regressor small))
        predictions (ml/transform (training-df) model)]
    (is (instance? XGBoostRegressionModel model))
    (is (every? double? (g/collect-col predictions :prediction)))
    (when-not (spark-at-least? "4.0")
      (is (round-trips? model XGBoostRegressionModel "regressor")))))

(deftest ranker-test
  (let [queries (g/with-column (training-df) :query (g/int (g/mod (g/monotonically-increasing-id) 4)))
        model   (ml/fit queries (ml/xgboost-ranker (assoc small :group-col "query")))]
    (is (instance? XGBoostRankerModel model))
    (is (= (g/count queries) (g/count (ml/transform queries model))))))
