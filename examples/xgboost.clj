(ns examples.xgboost
  (:require
    [zero-one.geni.core :as g]
    [zero-one.geni.ml :as ml]))

(def training (g/read-libsvm! "test/resources/sample_libsvm_data.txt"))

;; XGBoost Classifier
(def xgb-classifier-model
  (ml/fit
    training
    (ml/xgboost-classifier {:max-depth 2 :num-round 2})))

(-> training
    (ml/transform xgb-classifier-model)
    (g/select :label :probability)
    (g/limit 5)
    g/show)

;; XGBoost Regressor
(def xgb-regressor-model
  (ml/fit
    training
    (ml/xgboost-regressor {:max-depth 2 :num-round 2})))

(-> training
    (ml/transform xgb-regressor-model)
    (g/select :label :prediction)
    (g/limit 5)
    g/show)
