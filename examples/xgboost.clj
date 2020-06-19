(ns examples.xgboost
  (:require
    [zero-one.geni.core :as g]
    [zero-one.geni.ml :as ml]
    [zero-one.geni.test-resources :refer [spark]]))

;; XGBoost Classifier
(def training (g/read-libsvm! spark "test/resources/sample_libsvm_data.txt"))

(def xgb-model
  (ml/fit
    training
    (ml/xgboost-classifier {:max-depth 2 :num-round 2})))

(-> training
    (ml/transform xgb-model)
    (g/select "label" "probability")
    (g/limit 5)
    g/show)

;; XGBoost Regressor
(def training (g/read-libsvm! spark "test/resources/sample_libsvm_data.txt"))

(def xgb-model
  (ml/fit
    training
    (ml/xgboost-regressor {:max-depth 2 :num-round 2})))

(-> training
    (ml/transform xgb-model)
    (g/select "label" "prediction")
    (g/limit 5)
    g/show)
