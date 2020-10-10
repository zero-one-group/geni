(ns examples.regression
  (:require
    [zero-one.geni.core :as g]
    [zero-one.geni.ml :as ml]))

;; Linear Regression
(def training (g/read-libsvm! "test/resources/sample_libsvm_data.txt"))

(def lr (ml/linear-regression {:max-iter 10
                               :reg-param 0.8
                               :elastic-net-param 0.8}))

(def lr-model (ml/fit training lr))

(-> training
    (ml/transform lr-model)
    (g/select :label :prediction)
    (g/limit 5)
    g/show)

;;=>
;; +-----+----------+
;; |label|prediction|
;; +-----+----------+
;; |0.0  |0.57      |
;; |1.0  |0.57      |
;; |1.0  |0.57      |
;; |1.0  |0.57      |
;; |1.0  |0.57      |
;; +-----+----------+

(take 3 (ml/coefficients lr-model))
;;=> (0.0 0.0 0.0)

(ml/intercept lr-model)
;;=> 0.57

;; Random Forest Regression
(def data (g/read-libsvm! "test/resources/sample_libsvm_data.txt"))

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

;;=>
;; +----------+-----+
;; |prediction|label|
;; +----------+-----+
;; |0.0       |0.0  |
;; |0.0       |0.0  |
;; |0.0       |0.0  |
;; |0.0       |0.0  |
;; |0.25      |0.0  |
;; +----------+-----+
;; only showing top 5 rows

(println "RMSE:" (ml/evaluate predictions evaluator))
;;=> RMSE: 0.173685539601507

;; Survival Regression
(def train
  (g/table->dataset
   [[1.218 1.0 (g/dense 1.560 -0.605)]
    [2.949 0.0 (g/dense 0.346  2.158)]
    [3.627 0.0 (g/dense 1.380  0.231)]
    [0.273 1.0 (g/dense 0.520  1.151)]
    [4.199 0.0 (g/dense 0.795 -0.226)]]
   [:label :censor :features]))

(def quantile-probabilities [0.3 0.6])

(def aft
  (ml/aft-survival-regression
    {:quantile-probabilities quantile-probabilities
     :quantiles-col :quantiles}))

(def aft-model (ml/fit train aft))

(-> train (ml/transform aft-model) g/show)

;;=>
;; +-----+------+--------------+------------------+--------------------------------------+
;; |label|censor|features      |prediction        |quantiles                             |
;; +-----+------+--------------+------------------+--------------------------------------+
;; |1.218|1.0   |[1.56,-0.605] |5.71897948763501  |[1.1603238947151657,4.995456010274772]|
;; |2.949|0.0   |[0.346,2.158] |18.07652118149533 |[3.667545845471735,15.789611866277625]|
;; |3.627|0.0   |[1.38,0.231]  |7.381861804239101 |[1.4977061305190822,6.447962612338965]|
;; |0.273|1.0   |[0.52,1.151]  |13.577612501425284|[2.7547621481506823,11.8598722240697] |
;; |4.199|0.0   |[0.795,-0.226]|9.013097744073898 |[1.8286676321297806,7.87282650587843] |
;; +-----+------+--------------+------------------+--------------------------------------+
