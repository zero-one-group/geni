(ns examples.pipelien
  (:require
   [zero-one.geni.core :as g]
   [zero-one.geni.ml :as ml]))

(def training-set
  (g/table->dataset
   [[0 "a b c d e spark"  1.0]
    [1 "b d"              0.0]
    [2 "spark f g h"      1.0]
    [3 "hadoop mapreduce" 0.0]]
   [:id :text :label]))

(def pipeline
  (ml/pipeline
   (ml/tokenizer {:input-col :text
                  :output-col :words})
   (ml/hashing-tf {:num-features 1000
                   :input-col :words
                   :output-col :features})
   (ml/logistic-regression {:max-iter 10
                            :reg-param 0.001})))

(def model (ml/fit training-set pipeline))

(def test-set
  (g/table->dataset
   [[4 "spark i j k"]
    [5 "l m n"]
    [6 "spark hadoop spark"]
    [7 "apache hadoop"]]
   [:id :text]))

(-> test-set
    (ml/transform model)
    (g/select :id :text :probability :prediction)
    g/show)

;;=>
;; +---+------------------+----------------------------------------+----------+
;; |id |text              |probability                             |prediction|
;; +---+------------------+----------------------------------------+----------+
;; |4  |spark i j k       |[0.15964077387874104,0.8403592261212589]|1.0       |
;; |5  |l m n             |[0.8378325685476612,0.16216743145233875]|0.0       |
;; |6  |spark hadoop spark|[0.06926633132976262,0.9307336686702374]|1.0       |
;; |7  |apache hadoop     |[0.9821575333444208,0.01784246665557917]|0.0       |
;; +---+------------------+----------------------------------------+----------+
