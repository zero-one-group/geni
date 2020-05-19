(ns zero-one.geni.utils-test
  (:require
    [clojure.string]
    [midje.sweet :refer [facts fact =>]]
    [zero-one.geni.interop :as interop]
    [zero-one.geni.utils :refer [ensure-coll]])
  (:import
    (scala.collection Seq)))

(facts "On ensure-coll"
  (fact "should not change collections"
    (ensure-coll []) => []
    (ensure-coll #{"a"}) => #{"a"}
    (ensure-coll {:a 1}) => {:a 1}
    (ensure-coll (list 1 2)) => (list 1 2)
    (ensure-coll nil) => nil)
  (fact "should wrap non-collections in vector"
    (ensure-coll 1) => [1]
    (ensure-coll "a") => ["a"]))

(fact "On ->java"
  (let [converted (interop/->java Seq [0 1 2])]
    converted => #(instance? Seq %)))

(fact "On ->clojure"
  (let [data      [(interop/->scala-seq [1 2 3])]
        converted (interop/->clojure data)]
    converted => (map interop/->clojure data)))
