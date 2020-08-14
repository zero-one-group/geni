(ns zero-one.geni.rdd-test
  (:require
    [midje.sweet :refer [facts fact =>]]
    [zero-one.geni.rdd :as rdd]
    [zero-one.geni.aot-functions :as aot]))

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
        (rdd/reduce *)) => 120)
  (fact "map-to-pair + reduce-by-key + collect work"
    (-> (rdd/text-file "test/resources/rdd.txt")
        (rdd/map-to-pair aot/to-pair)
        (rdd/reduce-by-key +)
        rdd/collect) => [["Alice’s Adventures in Wonderland" 18]
                         ["at no cost and with" 27]
                         ["of anyone anywhere" 27]
                         ["by Lewis Carroll" 18]
                         ["Project Gutenberg’s" 9]
                         ["This eBook is for the use" 27]]
    (-> (rdd/text-file "test/resources/rdd.txt")
        (rdd/map-to-pair aot/to-pair)
        rdd/collect) => #(and (every? vector? %)
                              (every? (comp (partial = 2) count) %)
                              (every? (comp string? first) %)
                              (every? (comp (partial = 1) second) %))))
