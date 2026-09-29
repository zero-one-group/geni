(ns zero-one.geni.ml-xgb-test
  (:require
   [clojure.test :refer [deftest is]]
   [zero-one.geni.ml :as ml]
   [zero-one.geni.test-resources :refer [create-temp-file! libsvm-df]])
  (:import
   (ml.dmlc.xgboost4j.scala.spark XGBoostClassifier
                                  XGBoostRegressor)))

(deftest ^:slow xgb-native-test
  (is (nil?
       (let [estimator   (ml/xgboost-classifier {})
             model       (ml/fit (libsvm-df) estimator)
             temp-file   (.toString (create-temp-file! ""))]
         (ml/write-native-model! model temp-file)))))

(deftest instantiation-xgb-test
  (let [actual (ml/params (ml/xgboost-regressor {:num-round 890 :max-bin 222}))]
    (is (and (= (:num-round actual) 890)
             (= (:max-bin actual) 222))))
  (is (instance? XGBoostRegressor (ml/xgboost-regressor {})))
  (let [actual (ml/params (ml/xgboost-classifier {:eta 0.1 :max-bin 543}))]
    (is (and (= (:eta actual) 0.1)
             (= (:max-bin actual) 543))))
  (is (instance? XGBoostClassifier (ml/xgboost-classifier {}))))
