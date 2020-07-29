(ns examples.features
  (:require
    [zero-one.geni.core :as g]
    [zero-one.geni.ml :as ml]
    [zero-one.geni.test-resources :refer [libsvm-df]]))

;; Tokeniser, Hashing TF and IDF
(def sentence-data
  (g/table->dataset
    [[0.0 "Hi I heard about Spark"]
     [0.0 "I wish Java could use case classes"]
     [1.0 "Logistic regression models are neat"]]
    [:label :sentence]))

(def pipeline
  (ml/pipeline
    (ml/tokenizer {:input-col :sentence
                    :output-col :words})
    (ml/hashing-tf {:num-features 20
                    :input-col :words
                    :output-col :raw-features})
    (ml/idf {:input-col :raw-features
              :output-col :features})))

(def pipeline-model
  (ml/fit sentence-data pipeline))

(-> sentence-data
    (ml/transform pipeline-model)
    (g/collect-col :features))

;; PCA
(def dataframe
  (g/table->dataset
    [[(g/dense 0.0 1.0 0.0 7.0 0.0)]
     [(g/dense 2.0 0.0 3.0 4.0 5.0)]
     [(g/dense 4.0 0.0 0.0 6.0 7.0)]]
    [:features]))

(def pca
  (ml/fit dataframe (ml/pca {:input-col :features
                             :output-col :pca-features
                             :k 3})))

(-> dataframe
    (ml/transform pca)
    (g/collect-col :pca-features))

;; Standard Scaler
(def scaler
  (ml/standard-scaler {:input-col :features
                       :output-col :scaled-features
                       :with-std true
                       :with-mean false}))

(def scaler-model (ml/fit libsvm-df scaler))

(-> libsvm-df
    (ml/transform scaler-model)
    (g/limit 1)
    (g/collect-col :scaled-features))

;; Vector Assembler
(def dataset
  (g/table->dataset
    [[0 18 1.0 (g/dense 0.0 10.0 0.5) 1.0]]
    [:id :hour :mobile :user-features :clicked]))

(def assembler
  (ml/vector-assembler {:input-cols [:hour :mobile :user-features]
                        :output-col :features}))

(-> dataset
    (ml/transform assembler)
    (g/select :features :clicked)
    g/show)
