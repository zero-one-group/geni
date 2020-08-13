(ns zero-one.geni.rdd-test
  (:require
    [midje.sweet :refer [facts fact =>]]
    [zero-one.geni.rdd :as rdd]
    [zero-one.geni.rdd.function :as function]))

(facts "On basic RDD operations"
  (fact "map works"
    (-> (rdd/text-file "test/resources/rdd.txt")
        (rdd/map count)
        rdd/collect) => #(every? integer? %))
  (fact "reduce works"
    (-> (rdd/text-file "test/resources/rdd.txt" 1)
        (rdd/map count)
        (rdd/reduce +)) => 2709
    (-> (rdd/parallelise [1 2 3 4 5])
        (rdd/reduce *)) => 120))

(facts "On functions"
  (function/-init +) => [[] +]
  (.call (function/function +) 1) => 1
  (.call (function/function2 +) 1 2) => 3)

