(ns examples.classification
  (:require
   [zero-one.geni.core :as g]
   [zero-one.geni.ml :as ml]))

;; Logistic Regression
(def training (g/read-libsvm! "test/resources/sample_libsvm_data.txt"))

(def lr (ml/logistic-regression {:max-iter 10
                                 :reg-param 0.3
                                 :elastic-net-param 0.8}))

(def lr-model (ml/fit training lr))

(-> training
    (ml/transform lr-model)
    (g/select :label :probability)
    (g/limit 5)
    g/show)

;;=>
;; +-----+----------------------------------------+
;; |label|probability                             |
;; +-----+----------------------------------------+
;; |0.0  |[0.6764827243160599,0.32351727568394006]|
;; |1.0  |[0.22640965216205314,0.7735903478379468]|
;; |1.0  |[0.2210316383828499,0.7789683616171501] |
;; |1.0  |[0.2526490765347194,0.7473509234652805] |
;; |1.0  |[0.22494007343582254,0.7750599265641774]|
;; +-----+----------------------------------------+

(take 3 (ml/coefficients lr-model))
;;=> (-7.353983524188197E-5 -9.102738505589466E-5 -1.9467430546904298E-4)

(ml/intercept lr-model)
;;=> 0.22456315961250325

;; Gradient-Boosted Tree Classifier
(def data (g/read-libsvm! "test/resources/sample_libsvm_data.txt"))

(def split-data (g/random-split data [0.7 0.3]))
(def train-data (first split-data))
(def test-data (second split-data))

(def label-indexer
  (ml/fit data (ml/string-indexer {:input-col :label :output-col :indexed-label})))

(def feature-indexer
  (ml/fit data (ml/vector-indexer {:input-col :features
                                   :output-col :indexed-features
                                   :max-categories 4})))

(def pipeline
  (ml/pipeline
   label-indexer
   feature-indexer
   (ml/gbt-classifier {:label-col :indexed-label
                       :features-col :indexed-features
                       :max-iter 10
                       :feature-subset-strategy "auto"})
   (ml/index-to-string {:input-col :prediction
                        :output-col :predicted-label
                        :labels (.labels label-indexer)})))

(def model (ml/fit train-data pipeline))

(def predictions (ml/transform test-data model))

(def evaluator
  (ml/multiclass-classification-evaluator {:label-col :indexed-label
                                           :prediction-col :prediction
                                           :metric-name "accuracy"}))

(-> predictions
    (g/select :predicted-label :label)
    (g/order-by (g/rand)))

(println "Test error: " (- 1 (ml/evaluate predictions evaluator)))
;;=> Test error:  0.0
