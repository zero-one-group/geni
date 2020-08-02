(ns zero-one.geni.numpy-test
  (:require
    [midje.sweet :refer [throws fact =>]]
    [zero-one.geni.core :as g]
    [zero-one.geni.test-resources :refer [df-20]]))

(defn min-max-map [col]
  (-> (g/table->dataset (mapv vector (range 200)) [:idx])
      (g/with-column :x col)
      (g/agg {:min (g/min :x) :max (g/max :x)})
      g/first))

(fact "On random-int" ;:slow
  (min-max-map (g/random-int)) => #(and (pos? (:max %))
                                        (pos? (:min %))
                                        (integer? (:max %))
                                        (integer? (:min %)))
  (min-max-map (g/random-int -5 -2)) => #(and (= (:max %) -3)
                                              (= (:min %) -5)
                                              (integer? (:max %))
                                              (integer? (:min %)))
  (min-max-map (g/random-int 123)) => #(and (pos? (:max %))
                                            (pos? (:min %))
                                            (integer? (:max %))
                                            (integer? (:min %))))

(fact "On random-uniform" ;:slow
  (min-max-map (g/random-uniform)) => #(and (< 0.95 (:max %) 1.00)
                                            (< 0.00 (:min %) 0.05)
                                            (double? (:max %))
                                            (double? (:min %)))
  (min-max-map (g/random-uniform -0.5 -1.0)) => #(and (< -0.55 (:max %) -0.50)
                                                      (< -1.00 (:min %) -0.95)
                                                      (double? (:max %))
                                                      (double? (:min %)))
  (min-max-map (g/random-uniform 123)) => #(and (< 0.95 (:max %) 1.00)
                                                (< 0.00 (:min %) 0.05)
                                                (double? (:max %))
                                                (double? (:min %))))

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
