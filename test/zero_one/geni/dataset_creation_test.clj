(ns zero-one.geni.dataset-creation-test
  (:require
    [clojure.string :refer [includes?]]
    [midje.sweet :refer [facts fact =>]]
    [zero-one.geni.core :as g]
    [zero-one.geni.dataset-creation :as dataset-creation]
    [zero-one.geni.test-resources :refer [spark]])
  (:import
    (org.apache.spark.sql Dataset)))

(facts "On map->dataset"
  (fact "should create the right dataset"
    (let [dataset (dataset-creation/map->dataset
                    spark
                    {:a [1 4]
                     :b [2.0 5.0]
                     :c ["a" "b"]})]
      (instance? Dataset dataset) => true
      (g/column-names dataset) => ["a" "b" "c"]
      (g/collect-vals dataset) => [[1 2.0 "a"] [4 5.0 "b"]]))
  (fact "should create the right schema even with nils"
    (let [dataset (dataset-creation/map->dataset
                    spark
                    {:a [nil 4]
                     :b [2.0 5.0]})]
      (g/collect-vals dataset) => [[nil 2.0] [4 5.0]]))
  (fact "should create the right null column"
    (let [dataset (dataset-creation/map->dataset
                    spark
                    {:a [1 4]
                     :b [nil nil]})]
      (g/collect-vals dataset) => [[1 nil] [4 nil]]))
  (let [dataset (dataset-creation/table->dataset
                   spark
                   [[0.0 [0.5 10.0]]
                    [0.0 [1.5 20.0]]
                    [1.0 [1.5 30.0]]
                    [0.0 [3.5 30.0]]
                    [0.0 [3.5 40.0]]
                    [1.0 [3.5 40.0]]]
                   [:label :features])]
    (:features (g/dtypes dataset)) => #(includes? % "Vector")))

(facts "On records->dataset"
  (fact "should create the right dataset"
    (let [dataset (dataset-creation/records->dataset
                    spark
                    [{:a 1 :b 2.0 :c "a"}
                     {:a 4 :b 5.0 :c "b"}])]
      (instance? Dataset dataset) => true
      (g/column-names dataset) => ["a" "b" "c"]
      (g/collect-vals dataset) => [[1 2.0 "a"] [4 5.0 "b"]]))
  (fact "should create the right dataset even with missing keys"
    (let [dataset (dataset-creation/records->dataset
                    spark
                    [{:a 1 :c "a"}
                     {:a 4 :b 5.0}])]
      (g/column-names dataset) => ["a" "c" "b"]
      (g/collect-vals dataset) => [[1 "a" nil] [4 nil 5.0]])))

(facts "On table->dataset"
  (fact "should create the right dataset"
    (let [dataset (dataset-creation/table->dataset
                    spark
                    [[1 2.0 "a"]
                     [4 5.0 "b"]]
                    [:a :b :c])]
      (instance? Dataset dataset) => true
      (g/column-names dataset) => ["a" "b" "c"]
      (g/collect-vals dataset) => [[1 2.0 "a"] [4 5.0 "b"]])))
