(ns zero-one.geni.dataset-test
  (:require
    [midje.sweet :refer [facts fact =>]]
    [zero-one.geni.core :as g :refer [spark]]
    [zero-one.geni.dataset :as ds])
  (:import
    (org.apache.spark.sql Dataset)))

(facts "On map->dataset"
  (fact "should create the right dataset"
    (let [dataset (ds/map->dataset @spark {:a [1 4]
                                           :b [2.0 5.0]
                                           :c ["a" "b"]})]
      (instance? Dataset dataset) => true
      (g/column-names dataset) => ["a" "b" "c"]
      (g/collect-vals dataset) => [[1 2.0 "a"] [4 5.0 "b"]]))
  (fact "should create the right schema even with nils"
    (let [dataset (ds/map->dataset @spark {:a [nil 4]
                                           :b [2.0 5.0]})]
      (g/collect-vals dataset) => [[nil 2.0] [4 5.0]]))
  (fact "should create the right null column"
    (let [dataset (ds/map->dataset @spark {:a [1 4]
                                           :b [nil nil]})]
      (g/collect-vals dataset) => [[1 nil] [4 nil]])))
