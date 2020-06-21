(ns examples.regression
  (:require
    [zero-one.geni.core :as g]
    [zero-one.geni.ml :as ml]
    [zero-one.geni.test-resources :refer [spark]]))

;; Linear Regression
(def training (g/read-libsvm! spark "test/resources/sample_libsvm_data.txt"))

(def lr (ml/linear-regression {:max-iter 10
                               :reg-param 0.8
                               :elastic-net-param 0.8}))

(def lr-model (ml/fit training lr))

(-> training
    (ml/transform lr-model)
    (g/select :label :prediction)
    (g/limit 5)
    g/show)

(take 3 (ml/coefficients lr-model))

(ml/intercept lr-model)

;; Random Forest Regression
(def data (g/read-libsvm! spark "test/resources/sample_libsvm_data.txt"))

(def feature-indexer
  (ml/fit data (ml/vector-indexer {:input-col :features
                                   :output-col :indexed-features
                                   :max-categories 4})))

(def split-data (g/random-split data [0.7 0.3]))
(def train-data (first split-data))
(def test-data (second split-data))

(def pipeline
  (ml/pipeline
    feature-indexer
    (ml/random-forest-regressor {:label-col :label
                                 :features-col :indexed-features})))

(def model (ml/fit train-data pipeline))
(def predictions (ml/transform test-data model))
(def evaluator
  (ml/regression-evaluator {:label-col :label
                            :prediction-col :prediction
                            :metric-name "rmse"}))

(-> predictions
    (g/select :prediction :label)
    (g/show {:num-rows 5}))
(println "RMSE:" (ml/evaluate predictions evaluator))

;; Survival Regression
(def train
  (g/table->dataset
    spark
    [[1.218 1.0 [1.560 -0.605]]
     [2.949 0.0 [0.346  2.158]]
     [3.627 0.0 [1.380  0.231]]
     [0.273 1.0 [0.520  1.151]]
     [4.199 0.0 [0.795 -0.226]]]
    [:label :censor :features]))

(def quantile-probabilities [0.3 0.6])

(def aft
  (ml/aft-survival-regression
    {:quantile-probabilities quantile-probabilities
     :quantiles-col :quantiles}))

(def aft-model (ml/fit train aft))

(-> train (ml/transform aft-model) g/show)
