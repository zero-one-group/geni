(ns examples.classification
  (:require
    [zero-one.geni.core :as g]
    [zero-one.geni.ml :as ml]
    [zero-one.geni.test-resources :refer [spark]]))

(def training
  (g/table->dataset
    spark
    [[0  "a b c d e spark"  1.0]
     [1  "b d"              0.0]
     [2  "spark f g h"      1.0]
     [3  "hadoop mapreduce" 0.0]
     [4  "b spark who"      1.0]
     [5  "g d a y"          0.0]
     [6  "spark fly"        1.0]
     [7  "was mapreduce"    0.0]
     [8  "e spark program"  1.0]
     [9  "a e c l"          0.0]
     [10 "spark compile"    1.0]
     [11 "hadoop software"  0.0]]
    [:id :text :label]))

(def hashing-tf
  (ml/hashing-tf {:input-col :words :output-col :features}))

(def logistic-reg
  (ml/logistic-regression {:max-iter 10}))

(def pipeline
  (ml/pipeline
    (ml/tokeniser {:input-col :text :output-col :words})
    hashing-tf
    logistic-reg))

(def param-grid
  (ml/param-grid
    {hashing-tf {:num-features (mapv int [10 100 1000])}
     logistic-reg {:reg-param [0.1 0.01]}}))

(def cross-validator
  (ml/cross-validator {:estimator pipeline
                       :evaluator (ml/binary-classification-evaluator {})
                       :estimator-param-maps param-grid
                       :num-folds 2
                       :parallelism 2}))

(def cv-model (ml/fit training cross-validator))

(def testing
  (g/table->dataset
    spark
    [[4 "spark i j k"]
     [5 "l m n"]
     [6 "mapreduce spark"]
     [7 "apache hadoop"]]
    [:id :text]))

(-> testing
    (ml/transform cv-model)
    (g/select :id :text :probability :prediction)
    g/collect)
