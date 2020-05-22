(ns examples.features
  (:require
    [zero-one.geni.core :as g]
    [zero-one.geni.ml :as ml]
    [zero-one.geni.test-resources :refer [spark]]))

;; Logistic Regression
(def training (g/read-libsvm! spark "test/resources/sample_libsvm_data.txt"))

(def lr (ml/logistic-regression {:max-iter 10
                                 :reg-param 0.3
                                 :elastic-net-param 0.8}))

(def lr-model (ml/fit training lr))

(-> training
    (ml/transform lr-model)
    (g/select "label" "probability")
    (g/limit 5)
    g/show)

;; Gradient-Boosted Tree Classifier
(def data (g/read-libsvm! spark "test/resources/sample_libsvm_data.txt"))

(def split-data (g/random-split data [0.7 0.3]))
(def train-data (first split-data))
(def test-data (second split-data))

(def label-indexer
  (ml/fit data (ml/string-indexer {:input-col "label" :output-col "indexed-label"})))

(def pipeline
  (ml/pipeline
    label-indexer
    (ml/vector-indexer {:input-col "features"
                        :output-col "indexed-features"
                        :max-categories 4})
    (ml/gbt-classifier {:label-col "indexed-label"
                        :features-col "indexed-features"
                        :max-iter 10
                        :feature-subset-strategy "auto"})
    (ml/index-to-string {:input-col "prediction"
                         :output-col "predicted-label"
                         :labels (.labels label-indexer)})))

(def model (ml/fit train-data pipeline))

(-> train-data
    (ml/transform model)
    (g/select "predicted-label" "label")
    (g/show {:num-rows 5}))
