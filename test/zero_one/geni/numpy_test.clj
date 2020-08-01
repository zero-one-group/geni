(ns zero-one.geni.numpy-test
  (:require
    [midje.sweet :refer [fact =>]]
    [zero-one.geni.core :as g]
    [zero-one.geni.test-resources :refer [df-20]]))

(fact "On clip"
  (-> df-20
      (g/select (g/clip :Price 9e5 1.1e6))
      g/collect-vals
      flatten) => (fn [xs] (every? #(<= 9e5 % 1.1e6) xs)))
