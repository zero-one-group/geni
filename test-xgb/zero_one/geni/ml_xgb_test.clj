(ns zero-one.geni.ml-xgb-test
  (:require
   [midje.sweet :refer [fact =>]]
   [zero-one.geni.ml :as ml]
   [zero-one.geni.test-resources :refer [create-temp-file! libsvm-df]])
  (:import
   (ml.dmlc.xgboost4j.scala.spark XGBoostClassifier
                                  XGBoostRegressor)))

(fact "On XGB native" :slow
  (let [estimator   (ml/xgboost-classifier {})
        model       (ml/fit (libsvm-df) estimator)
        temp-file   (.toString (create-temp-file! ""))]
    (ml/write-native-model! model temp-file)) => nil?)

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
