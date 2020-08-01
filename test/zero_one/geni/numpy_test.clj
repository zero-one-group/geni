(ns zero-one.geni.numpy-test
  (:require
    [midje.sweet :refer [throws fact =>]]
    [zero-one.geni.core :as g]
    [zero-one.geni.test-resources :refer [df-20]]))

(fact "On random-choice" :slow
  (-> (g/table->dataset (mapv vector (range 100)) [:idx])
      (g/with-column :rand-choice (g/random-choice [(g/lit "abc")
                                                    (g/lit "def")
                                                    (g/lit "ghi")]))
      (g/collect-col :rand-choice)
      set) => #{"abc" "def" "ghi"}
  (-> (g/table->dataset (mapv vector (range 2000)) [:idx])
      (g/with-column :rand-choice (g/random-choice [0 1 2 3] [0.5 0.3 0.15 0.05]))
      (g/select :rand-choice)
      g/value-counts
      g/collect) => #(= (mapv :rand-choice %) [0 1 2 3])
  (g/random-choice [0] [2.0]) => (throws AssertionError)
  (g/random-choice [] [1.0]) => (throws AssertionError)
  (g/random-choice [0 1] [-1.0 2.0]) => (throws AssertionError))

(fact "On clip" :slow
  (-> df-20
      (g/select (g/clip :Price 9e5 1.1e6))
      g/collect-vals
      flatten) => (fn [xs] (every? #(<= 9e5 % 1.1e6) xs)))
